require 'pg'
require 'json'

class Hiera
  module Backend
    class Psql_backend

      class << self
        # Get a config key for this backend
        def config key
          Config[:psql][key.to_sym] unless Config[:psql].nil?
        end


        def connection
          @connection ||= PGconn.open(config :connection)
        end

      end


      def initialize
        Hiera.debug("Hiera PostgreSQL backend starting")
      end


      def lookup(key, scope, order_override, resolution_type)
        answer = nil

        # Set environment from config
        Hiera.debug("Looking up #{key} in PostgreSQL backend")

        Backend.datasources(scope, order_override) do |source|
          environment = Hiera::Backend.parse_string(config(:environment), scope)
          if environment.nil?
            query = 'SELECT value->$2 FROM data WHERE path=$1'
            arguments = [source, key]
          else
            query = 'SELECT value->$3 FROM data WHERE environment=$1 ' \
                'AND path=$2'
            arguments = [environment, source, key]
          end

          connection.exec query, arguments do |result|
            # Get value from result
            entry = result.values.flatten.first
            unless entry.nil?
              # Extra logging that we found the key. This can be outputted
              # multiple times if the resolution type is array or hash but that
              # should be expected as the logging will then tell the user ALL the
              # places where the key is found.
              Hiera.debug "Found #{key} in #{source}"

              # for array resolution we just append to the array whatever
              # we find, we then goes onto the next file and keep adding to
              # the array
              #
              # for priority searches we break after the first found data item
              new_answer = Backend.parse_answer JSON.load(entry), scope

              unless new_answer.nil?
                case resolution_type
                  when :array
                    raise Exception, "Hiera type mismatch: expected Array " \
                        "and got #{new_answer.class}" unless
                            new_answer.kind_of? Array or
                                new_answer.kind_of? String

                    answer ||= []
                    answer << new_answer
                  when :hash
                    raise Exception, "Hiera type mismatch: expected Hash " \
                        "and got #{new_answer.class}" unless
                            new_answer.kind_of?  Hash
                    answer ||= {}
                    answer = Backend.merge_answer(new_answer, answer)
                  else
                    # Stop further search for priority resolution
                    answer ||= new_answer
                    break
                end
              end

            end
          end
        end
        answer
      end


      private

      def config key
        self.class.config key
      end


      def connection
        self.class.connection
      end
    end
  end
end

