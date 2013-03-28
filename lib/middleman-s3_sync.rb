require 'middleman-core'
require 'fog'
require 'parallel'
require 'digest/md5'
require 'middleman-s3_sync/version'
require 'middleman-s3_sync/commands'

::Middleman::Extensions.register(:s3_sync, '>= 3.0.0') do
  require 'middleman-s3_sync/extension'
  ::Middleman::S3Sync
end

module Middleman
  module S3Sync
    class << self
      def sync
        puts "Gathering local files."

        local_files = (Dir[options.build_dir + "/**/*"] + Dir[options.build_dir + "/**/.*"])
          .reject { |f| File.directory?(f) }
          .map { |f| f.gsub(/^#{options.build_dir}\//, '') }
        puts "Gathering remote files from #{options.bucket}"
        remote_files = bucket.files.map { |f| f.key }

        if options.force
          files_to_push = local_files
        else
          # First pass on the set of files to work with.
          puts "Determine files to add to #{options.bucket}."
          files_to_push = local_files - remote_files
          if options.delete
            puts "Determine which files to delete from #{options.bucket}"
            files_to_delete = remote_files - local_files
          end
          files_to_evaluate = local_files - files_to_push

          # No need to evaluate the files that are newer on S3 than the local files.
          puts "Determine which local files are newer than their S3 counterparts"
          files_to_reject = []
          Parallel.each(files_to_evaluate, :in_threads => 4) do |f|
            print '.'
            local_mtime = File.mtime("#{options.build_dir}/#{f}")
            remote_mtime = s3_files.get(f).last_modified
            files_to_reject << f if remote_mtime >= local_mtime
          end

          files_to_evaluate = files_to_evaluate - files_to_reject

          # Are the files different? Use MD5 to see
          if (files_to_evaluate.size > 0)
            puts "\n\nDetermine which remaining files are actually different than their S3 counterpart."
            Parallel.each(files_to_evaluate, :in_threads => 4) do |f|
              print '.'
              local_md5 = Digest::MD5.hexdigest(File.read("#{options.build_dir}/#{f}"))
              remote_md5 = s3_files.get(f).etag
              files_to_push << f if local_md5 != remote_md5
            end
          end
        end

        if files_to_push.size > 0
          puts "\n\nReady to apply updates to #{options.bucket}."
          files_to_push.each do |f|
            if remote_files.include?(f)
              puts "Updating #{f}"
              file = s3_files.get(f)
              file.body = File.open("#{options.build_dir}/#{f}")
              file.public = true
              file.content_type = MIME::Types.of(f).first
              if policy = options.caching_policy_for(file.content_type)
                file.cache_control = policy.cache_control if policy.cache_control
                file.expires = policy.expires if policy.expires
              end

              file.save
            else
              puts "Creating #{f}"
              file_hash = {
                :key => f,
                :body => File.open("#{options.build_dir}/#{f}"),
                :public => true,
                :acl => 'public-read',
                :content_type => MIME::Types.of(f).first
              }

              # Add cache-control headers
              if policy = options.caching_policy_for(file_hash[:content_type])
                file_hash[:cache_control] = policy.cache_control if policy.cache_control
                file_hash[:expires] = policy.expires if policy.expires
              end

              file = bucket.files.create(file_hash)
            end
          end
        else
          puts "\n\nNo files to update."
        end

        if options.delete
          files_to_delete.each do |f|
            puts "Deleting #{f}"
            file = s3_files.get(f)
            file.destroy
          end
        end
      end

      protected
      def connection
        @connection ||= Fog::Storage.new({
          :provider => 'AWS',
          :aws_access_key_id => options.aws_access_key_id,
          :aws_secret_access_key => options.aws_secret_access_key,
          :region => options.region
        })
      end

      def bucket
        @bucket ||= connection.directories.get(options.bucket)
      end

      def s3_files
        @s3_files ||= bucket.files
      end
    end
  end
end

