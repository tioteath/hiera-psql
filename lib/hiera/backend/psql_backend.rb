class Hiera
  module Backend
    class Psql_backend
      def initialize
        require 'pg'
        require 'json'

        Hiera.debug("Hiera PostgreSQL backend starting")
      end


      def lookup(key, scope, order_override, resolution_type)
        answer = nil

        Hiera.debug("Looking up #{key} in PostgreSQL backend")

        Backend.datasources(scope, order_override) do |source|
          result = connection.exec(
              "SELECT value->$2 FROM data WHERE path=$1", [source, key]).first
          # Extra logging that we found the key. This can be outputted
          # multiple times if the resolution type is array or hash but that
          # should be expected as the logging will then tell the user ALL the
          # places where the key is found.
          Hiera.debug "Looking for data source #{source}"

          next unless result.is_a? Hash
          next unless result.has_key? '?column?'
          value = result['?column?']
          next if value.nil?

          data = JSON.load value

          # for array resolution we just append to the array whatever
          # we find, we then goes onto the next file and keep adding to
          # the array
          #
          # for priority searches we break after the first found data item
          new_answer = Backend.parse_answer data, scope

          if resolution_type.eql? :array
            unless new_answer.kind_of? Array or new_answer.kind_of? String
              raise Exception, "Hiera type mismatch: " \
                  "expected Array and got #{new_answer.class}"
            end

            answer ||= []
            answer << new_answer

          elsif resolution_type.eql? :hash
            unless new_answer.kind_of? Hash
              raise Exception, "Hiera type mismatch: " \
                "expected Hash and got #{new_answer.class}"
            end

            answer ||= {}
            answer = Backend.merge_answer new_answer, answer

          else
            answer = new_answer
            break
          end

        end

        return answer
      end


      private

      # Get a config key for this backend
      def self.config key
        Config[:psql][key.to_sym]
      end


      def self.connection
        @connection ||= PGconn.open config :connection
      end


      def connection
        self.class.connection
      end
    end
  end
end
