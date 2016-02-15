module Clockwork
  module DatabaseEvents
    class Event < Clockwork::Event

      attr_accessor :sync_performer, :at

      def initialize(manager, period, job, block, sync_performer, options={})
        super(manager, period, job, block, options)
        @sync_performer = sync_performer
        @sync_performer.register(self, job)
      end

      def name
        (job.respond_to?(:name) && job.name) ? job.name : "#{job.class}:#{job.id}"
      end

      def to_s
        name
      end

      def changed?(model)
        name_changed?(model) || frequency_changed?(model) || at_changed?(model)
      end

      private
      def at_changed?(model)
        @at != At.parse(model.at)
      end

      def name_changed?(model)
        job.respond_to?(:name) && job.name != model.name
      end

      def frequency_changed?(model)
        @period != model.frequency
      end
    end
  end
end
