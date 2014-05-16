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
          connection.exec "SELECT value->$2 FROM data WHERE path=$1",
              [source, key] do |result|
            # Extra logging that we found the key. This can be outputted
            # multiple times if the resolution type is array or hash but that
            # should be expected as the logging will then tell the user ALL the
            # places where the key is found.
            Hiera.debug "Looking for data source #{source}"

            entry = result.first

            next unless entry.is_a? Hash
            next unless entry.has_key? '?column?'

            value = entry['?column?']
            next unless value

            data = JSON.load value
            next if data.empty? || data.nil?

            # for array resolution we just append to the array whatever
            # we find, we then goes onto the next file and keep adding to
            # the array
            #
            # for priority searches we break after the first found data item
            new_answer = Backend.parse_answer data, scope

            if resolution_type.eql? :array
              raise Exception, "Hiera type mismatch: " \
                  "expected Array and got #{new_answer.class}" unless
                      new_answer.kind_of? Array or
                          new_answer.kind_of? String
              answer ||= []
              answer << new_answer

            elsif resolution_type.eql? :hash
              raise Exception, "Hiera type mismatch: " \
                      "expected Hash and got #{new_answer.class}" unless
                          new_answer.kind_of? Hash
              answer ||= {}
              answer = Backend.merge_answer new_answer, answer

            else
              answer = new_answer
            end

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
