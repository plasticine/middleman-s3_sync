require 'middleman-core'
require 'map'

module Middleman
  module S3Sync
    class Options < Struct.new(
      :prefix,
      :bucket,
      :region,
      :aws_access_key_id,
      :aws_secret_access_key,
      :after_build,
      :delete,
      :existing_remote_file,
      :build_dir,
      :force,
      :prefer_gzip,
      :exclude
    )

      def set_default_headers(content_type, options)
        headers[:default] = FileHeaders.new(options)
      end

      def set_headers(content_type, options)
        headers[content_type.to_s] = FileHeaders.new(options)
      end

      def get_headers(content_type)
        headers.fetch(content_type.to_s, default_headers)
      end

      def default_headers
        headers[:default]
      end

      def headers
        @headers ||= Map.new
      end

      protected

      class FileHeaders
        attr_accessor :headers

        def initialize(options)
          @headers = Map.from_hash(options)
        end

        def content_encoding
          headers.has_key?(:content_encoding) ? headers['content_encoding'] : false
        end

        def cache_control
          policy = []
          policy << "max-age=#{headers.cache_control.max_age}" if headers.cache_control.has_key?(:max_age)
          policy << "s-maxage=#{s_maxage}" if headers.cache_control.has_key?(:s_maxage)
          policy << "public" if headers.cache_control.fetch(:public, false)
          policy << "private" if headers.cache_control.fetch(:private, false)
          policy << "no-cache" if headers.cache_control.fetch(:no_cache, false)
          policy << "no-store" if headers.cache_control.fetch(:no_store, false)
          policy << "must-revalidate" if headers.cache_control.fetch(:must_revalidate, false)
          policy << "proxy-revalidate" if headers.cache_control.fetch(:proxy_revalidate, false)
          if policy.empty?
            nil
          else
            policy.join(", ")
          end
        end

        def expires
          if expiration = headers.fetch(:expires, nil)
            CGI.rfc1123_date(expiration)
          end
        end
      end
    end

    class << self
      def options
        @@options
      end

      def registered(app, options_hash = {}, &block)
        options = Options.new(options_hash)
        yield options if block_given?

        @@options = options

        app.send :include, Helpers

        app.after_configuration do |config|
          options.build_dir ||= build_dir
        end

        app.after_build do |builder|
          ::Middleman::S3Sync.sync if options.after_build
        end
      end

      alias :included :registered

      module Helpers
        def options
          ::Middleman::S3Sync.options
        end

        def set_headers(content_type, metadata={})
          options.set_headers(content_type, metadata)
        end

        def set_default_headers(metadata={})
          set_headers(:default, metadata)
        end
      end
    end
  end
end
