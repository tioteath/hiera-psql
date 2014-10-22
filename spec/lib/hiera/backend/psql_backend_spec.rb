require 'spec_helper'
require 'hiera/backend/psql_backend'

class Hiera
  module Backend
    describe Psql_backend do
      before do
        Config.load :psql => {
            :environment => 'test',
            :connection => 'asdfas'
        }
        allow(Hiera).to receive :debug
        allow(Hiera).to receive :warn

        @connection_mock = double('connection').as_null_object
        allow(Psql_backend).to receive(:connection).and_return @connection_mock

        @sql_query  = 'SELECT value->$3 FROM data WHERE environment=$1 AND ' \
            'path=$2'

        @backend = Psql_backend.new
      end


      describe '#initialize' do
        it 'prints debug through Hiera' do
          expect(Hiera).to receive(:debug).
              with 'Hiera PostgreSQL backend starting'
          Psql_backend.new
        end
      end


      describe '#lookup' do

        it 'looks for data in all sources' do
          expect(Backend).to receive(:datasources).
              and_yield(['one']).and_yield ['two']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with @sql_query, ['test', ['one'], anything]

          expect(@connection_mock).to receive(:exec).once.ordered.
              with @sql_query, ['test', ['two'], anything]

          @backend.lookup(:key, {}, nil, :priority)
        end


        it 'picks data earliest source that has it for priority searches' do
          expect(Backend).to receive(:datasources).
              and_yield(['one']).and_yield ['two']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values(['patate', 'asdf'])

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['two'], anything]).
              and_yield mock_values([])

          expect(@backend.lookup(:key, {}, nil, :priority)).
              to eq 'patate'
        end


        it 'returns nil for missing path/value' do
          expect(Backend).to receive(:datasources).with({}, :override).
              and_yield ['one']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], 'key']).
              and_yield mock_values([])

          @backend.lookup('key', {}, :override, :priority)
        end


        it 'builds an array of all data sources for array searches' do
          expect(Backend).to receive(:datasources).
              and_yield(['one']).and_yield ['two']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values('answer')

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['two'], anything]).
              and_yield mock_values('answer')

          expect(@backend.lookup(:key, {}, nil, :array)).
              to eq ['answer', 'answer']
        end


        it 'ignores empty hash of data sources for hash searches' do
          expect(Backend).to receive(:datasources).
              and_yield(['one']).and_yield ['two']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'],
              anything]).and_yield mock_values({})

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['two'], anything]).
              and_yield mock_values({ :a => 'answer' })

          expect(@backend.lookup(:key, {}, nil, :hash)).
              to eq 'a' => 'answer'
        end


        it 'builds a merged hash of data sources for hash searches' do
          expect(Backend).to receive(:datasources).
              and_yield(['one']).and_yield ['two']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values({:a => 'answer'})

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['two'], anything]).
              and_yield mock_values({ :b => 'answer', :a => 'wrong'})

          expect(@backend.lookup(:key, {}, nil, :hash)).
              to eq 'a' => 'answer', 'b' => 'answer'
        end


        it 'fails when trying to << a Hash' do
          expect(Backend).to receive(:datasources).
              and_yield(['one']).and_yield ['two']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values([['a', 'answer']])

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['two'], anything]).
              and_yield mock_values({ :a => 'answer' })

          expect { @backend.lookup(:key, {}, nil, :array) }.
              to raise_error Exception,
                  'Hiera type mismatch: expected Array and got Hash'
        end


        it 'fails when trying to merge an Array' do
          expect(Backend).to receive(:datasources).
              and_yield(['one']).and_yield ['two']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values({:a => 'answer'})

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['two'], anything]).
              and_yield mock_values([['a', 'answer']])

          expect { @backend.lookup(:key, {}, nil, :hash) }.
              to raise_error Exception,
                  'Hiera type mismatch: expected Hash and got Array'
        end


        it 'parses the answer for scope variables' do
          expect(Backend).to receive(:datasources).and_yield ['one']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values("test_%{rspec}")

          expect(@backend.lookup(:key, {'environment' => 'test',
              'rspec' => 'test'},  nil, :priority)).to eq('test_test')
        end


        it 'retains the data types found in value' do
          expect(Backend).to receive(:datasources).exactly(3).and_yield ['one']

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values('string')

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values(true)

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values(1)

          expect(@backend.lookup 'stringval', {}, nil, :priority).
              to eq 'string'
          expect(@backend.lookup 'boolval', {}, nil, :priority).
              to eq true
          expect(@backend.lookup 'numericval', {}, nil, :priority).
              to eq 1
        end


        it 'returns nil for array searches if no data found in data sources' do
          expect(Backend).to receive(:datasources).
              and_yield(['one']).and_yield(['two']).and_yield(['three'])

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['one'], anything]).
              and_yield mock_values(nil)

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['two'], anything]).
              and_yield mock_values(nil)

          expect(@connection_mock).to receive(:exec).once.ordered.
              with(@sql_query, ['test', ['three'], anything]).
              and_yield mock_values(nil)

          expect(@backend.lookup(:key, {}, nil, :array)).
              to eq nil
        end


        it 'uses simple sql query in case environment is not set' do
          Config.load :psql => {
              :connection => 'asdfas'
          }
          expect(Backend).to receive(:datasources).and_yield(['one'])

          expect(@connection_mock).to receive(:exec).once.ordered.
              with('SELECT value->$2 FROM data WHERE path=$1',
              [['one'], anything]).and_yield mock_values(nil)

          expect(@backend.lookup(:key, {}, nil, :array)).
              to eq nil
        end

      end
    end
  end
end
