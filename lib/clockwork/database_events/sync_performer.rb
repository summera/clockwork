require_relative '../database_events'

module Clockwork

  module DatabaseEvents

    class SyncPerformer

      PERFORMERS = []

      def self.setup(options={}, &block)
        model_class = options.fetch(:model) { raise KeyError, ":model must be set to the model class" }
        every = options.fetch(:every) { raise KeyError, ":every must be set to the database sync frequency" }

        sync_performer = self.new(model_class, &block)

        # create event that syncs clockwork events with events coming from database-backed model
        Clockwork.manager.every every, "sync_database_events_for_model_#{model_class}" do
          sync_performer.sync
        end
      end

      def initialize(model_class, &proc)
        @model_class = model_class
        @block = proc
        @database_event_registry = Registry.new

        PERFORMERS << self
      end

      # delegates to Registry
      def register(event, model)
        @database_event_registry.register(event, model)
      end

      # Ensure clockwork events reflect events from database-backed model
      # Adds any new events, modifies updated ones, and delets removed ones
      def sync
        model_ids_that_exist = []

        @model_class.all.each do |model|
          model_ids_that_exist << model.id
          if changed?(@database_event_registry.event_for(model), model)
            create_or_recreate_event(model)
          end
        end
        @database_event_registry.unregister_all_except(model_ids_that_exist)
      end

      private
      def changed?(event, model)
        return true if event.nil?
        event.changed?(model) #|| ats_have_changed?(model)
      end

      def at_strings_for(model)
        model.at.to_s.empty? ? nil : model.at.split(',').map(&:strip)
      end

      def create_or_recreate_event(model)
        if @database_event_registry.event_for(model)
          @database_event_registry.unregister(model)
        end

        options = {
          :from_database => true,
          :sync_performer => self,
          :at => at_strings_for(model)
        }

        options[:tz] = model.tz if model.respond_to?(:tz)

        # we pass actual model instance as the job, rather than just name
        Clockwork.manager.every model.frequency, model, options, &@block
      end
    end

  end
end
