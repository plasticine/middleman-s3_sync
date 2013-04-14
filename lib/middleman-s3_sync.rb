require 'middleman-core'
require 'fog'
require 'parallel'
require 'ruby-progressbar'
require 'colorize'
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
        @files_to_push = []
        @files_to_delete = []

        get_local_files()
        get_remote_files()
        apply_file_exclusion()
        evaluate_files()
        process_files()
        delete_orphan_files()
      end

      private

      def say_status(status)
        puts :s3_sync.to_s.rjust(12).light_green + "  #{status}"
      end

      def build_dir_file(f)
        File.join(options.build_dir, f)
      end

      def get_local_files
        say_status "building local file list"
        @local_files = (Dir[options.build_dir + "/**/*"] + Dir[options.build_dir + "/**/.*"])
          .reject{|f| File.directory?(f)}
          .map{|f| f.gsub(/^#{options.build_dir}\//, '')}
      end

      def apply_file_exclusion
        if options.exclude
          say_status "filtering local files matching #{options.exclude}"
          @local_files.reject!{|f| f.match Regexp.union(options.exclude)}
        end
      end

      def get_remote_files
        say_status "building remote file list"
        @remote_files = bucket.files.map{|f| f.key}
      end

      def evaluate_files
        say_status "evaluating files to be uploaded"
        if options.force
          say_status "force including all files (--force)".light_yellow
          @files_to_push = @local_files
        else
          # First pass on the set of files to work with.
          @files_to_push = @local_files - @remote_files
        end

        @files_to_delete = @remote_files - @local_files if options.delete
        @files_to_evaluate = @local_files - @files_to_push

        # No need to evaluate the files that are newer on S3 than the local files.
        @files_to_reject = []
        Parallel.each(@files_to_evaluate, :in_threads => 20) do |f|
          local_mtime = File.mtime(build_dir_file(f))
          remote_mtime = s3_files.get(f).last_modified
          @files_to_reject << f if remote_mtime >= local_mtime
        end

        @files_to_evaluate = @files_to_evaluate - @files_to_reject

        # Are the files different? Use MD5 to see
        if (@files_to_evaluate.size > 0)
          reject_gzipped_files() if options.prefer_gzip
          compare_file_md5()
        end

        say_status "#{@files_to_push.size} files found to upload to remote"
        say_status "#{@files_to_delete.size} files found to delete from remote"
      end

      def reject_gzipped_files
        @files_to_evaluate.reject!{|f| File.extname(build_dir_file(f)) == ".gz"}
      end

      def compare_file_md5
        Parallel.each(@files_to_evaluate, :in_threads => 16) do |f|
          local_md5 = Digest::MD5.hexdigest(File.read(read_local_file(f)))
          remote_md5 = s3_files.get(f).etag
          @files_to_push << f if local_md5 != remote_md5
        end
      end

      def process_files
        if @files_to_push.size > 0
          Parallel.each(@files_to_push) do |f|
            if @remote_files.include?(f)
              update_remote_file(f)
            else
              create_remote_file(f)
            end
          end
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
        say_status "updating".light_blue + " #{f}"
        file = s3_files.get(f)
        file.body = read_local_file(f)
        file.public = true
        file.content_type = MIME::Types.of(f).first

        if metadata = options.get_headers(file.content_type)
          file.cache_control    = metadata.cache_control if metadata.cache_control
          file.expires          = metadata.expires if metadata.expires
          file.content_encoding = metadata.content_encoding if metadata.content_encoding
        end
        file.save
      end

      def create_remote_file(f)
        say_status "creating".light_blue + " #{f}"
        file_hash = {
          :key => f,
          :body => read_local_file(f),
          :public => true,
          :acl => 'public-read',
          :content_type => MIME::Types.of(f).first
        }

        # Add cache-control headers
        if metadata = options.get_headers(file_hash[:content_type])
          file_hash[:cache_control] = metadata.cache_control if metadata.cache_control
          file_hash[:expires] = metadata.expires if metadata.expires
          file_hash[:content_encoding] = metadata.content_encoding if metadata.content_encoding
        end
        file = bucket.files.create(file_hash)
      end

      def delete_orphan_files
        if options.delete
          Parallel.each(@files_to_delete) do |f|
            say_status "deleting".light_red + " #{f}"
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
