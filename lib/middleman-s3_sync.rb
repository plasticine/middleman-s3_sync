require 'middleman-core'
require 'fog'
require 'parallel'
require 'ruby-progressbar'
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
        get_local_files()
        get_remote_files()
        evaluate_files()
        process_files()
        delete_orphan_files()
      end

      private

      def build_dir_file(f)
        File.join(options.build_dir, f)
      end

      def get_local_files
        puts "Gathering local files"
        @local_files = (Dir[options.build_dir + "/**/*"] + Dir[options.build_dir + "/**/.*"])
          .reject{|f| File.directory?(f)}
          .map{|f| f.gsub(/^#{options.build_dir}\//, '')}
      end

      def get_remote_files
        puts "Gathering remote files from #{options.bucket}"
        @remote_files = bucket.files.map{|f| f.key}
      end

      def evaluate_files
        if options.force
          @files_to_push = @local_files
        else
          # First pass on the set of files to work with.
          puts "Determine files to add to #{options.bucket}."
          @files_to_push = @local_files - @remote_files
          if options.delete
            puts "Determine which files to delete from #{options.bucket}"
            @files_to_delete = @remote_files - @local_files
          end
          @files_to_evaluate = @local_files - @files_to_push

          # No need to evaluate the files that are newer on S3 than the local files.
          puts "Determine which local files are newer than their S3 counterparts"
          @files_to_reject = []
          Parallel.each(@files_to_evaluate, :in_threads => 16) do |f|
            local_mtime = File.mtime(build_dir_file(f))
            remote_mtime = s3_files.get(f).last_modified
            @files_to_reject << f if remote_mtime >= local_mtime
          end

          @files_to_evaluate = @files_to_evaluate - @files_to_reject

          # Are the files different? Use MD5 to see
          if (@files_to_evaluate.size > 0)
            exclude_gzipped_files() if options.prefer_gzip
            compare_file_md5()
          end
        end
      end

      def exclude_gzipped_files
        @files_to_evaluate.reject!{|f|
          puts build_dir_file(f), File.extname(build_dir_file(f))
          File.extname(build_dir_file(f)) == ".gz"
        }
      end

      def compare_file_md5
        puts "Determine which remaining files are actually different than their S3 counterpart."
        Parallel.each(@files_to_evaluate, :in_threads => 16) do |f|
          local_md5 = Digest::MD5.hexdigest(File.read(read_local_file(f)))
          remote_md5 = s3_files.get(f).etag
          @files_to_push << f if local_md5 != remote_md5
        end
      end

      def process_files
        if @files_to_push.size > 0
          puts "Ready to apply updates to #{options.bucket}."
          # progress = ProgressBar.create(:title => "Uploading", :total => @files_to_push.size, :format => '%a |%B>%i| %p%% %t')
          # Parallel.each(@files_to_push, :in_threads => 16, :finish => lambda{|i, item| progress.increment}) do |f|
          Parallel.each(@files_to_push, :in_threads => 0) do |f|
            if @remote_files.include?(f)
              update_remote_file(f)
            else
              create_remote_file(f)
            end
          end
        else
          puts "No files to update."
        end
      end

      def read_local_file(f)
        if options.prefer_gzip and File.exist?("#{build_dir_file(f)}.gz")
          File.open("#{build_dir_file(f)}.gz")
        else
          File.open(build_dir_file(f))
        end
      end

      def update_remote_file(f)
        puts "Updating #{f}"
        file = s3_files.get(f)
        file.body = read_local_file(f)
        file.public = true
        file.content_type = MIME::Types.of(f).first

        if policy = options.caching_policy_for(file.content_type)
          file.cache_control = policy.cache_control if policy.cache_control
          file.expires = policy.expires if policy.expires
        end

        file.save
      end

      def create_remote_file(f)
        puts "Creating #{f}"
        file_hash = {
          :key => f,
          :body => read_local_file(f),
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

      def delete_orphan_files
        if options.delete
          @files_to_delete.each do |f|
            puts "Deleting #{f}"
            s3_files.get(f).destroy
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

