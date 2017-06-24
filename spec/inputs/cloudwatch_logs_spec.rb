# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/cloudwatch_logs"

describe LogStash::Inputs::CloudWatch_Logs do
  before do
    Aws.config[:stub_responses] = true
    Thread.abort_on_exception = true
  end

  describe '#register' do
    let(:config) {
      {
          'access_key_id' => '1234',
          'secret_access_key' => 'secret',
          'log_group' => ['sample-log-group'],
          'region' => 'us-east-1'
      }
    }
    subject {LogStash::Inputs::CloudWatch_Logs.new(config)}

    it "registers succesfully" do
      expect {subject.register}.to_not raise_error
    end
  end
end
