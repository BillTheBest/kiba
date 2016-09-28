module Kiba
  module Runner
    # allow to handle a block form just like a regular transform
    class AliasingProc < Proc
      alias_method :process, :call
    end

    class ComponentInstantiationError < StandardError; end

    def run(control)
      # TODO: add a dry-run (not instantiating mode) to_instances call
      # that will validate the job definition from a syntax pov before
      # going any further. This could be shared with the parser.
      run_pre_processes(control)
      process_rows(
        to_instances('source', control.sources),
        to_instances('transform', control.transforms, true),
        to_instances('destination', control.destinations)
      )
      # TODO: when I add post processes as class, I'll have to add a test to
      # make sure instantiation occurs after the main processing is done (#16)
      run_post_processes(control)
    end

    def run_pre_processes(control)
      to_instances('pre_process', control.pre_processes, true, false).each(&:call)
    end

    def run_post_processes(control)
      to_instances('post_process', control.post_processes, true, false).each(&:call)
    end

    def process_rows(sources, transforms, destinations)
      sources.each do |source|
        source.each do |row|
          transforms.each do |transform|
            row = transform.process(row)
            break unless row
          end
          next unless row
          destinations.each do |destination|
            destination.write(row)
          end
        end
      end
      destinations.each(&:close)
    end

    # not using keyword args because JRuby defaults to 1.9 syntax currently
    def to_instances(context, definitions, allow_block = false, allow_class = true)
      definitions.map do |definition|
        to_instance(
          context,
          *definition.values_at(:klass, :args, :block),
          allow_block, allow_class
        )
      end
    end

    def to_instance(context, klass, args, block, allow_block, allow_class)
      if klass
        fail "Class form is not allowed for #{context}" unless allow_class
        begin
          klass.new(*args)
        rescue => e
          # TODO: propagate inner exception (a la nestegg)
          raise ComponentInstantiationError.new("Kiba #{context} #{klass} instantiation failed (#{e.inspect})")
        end
      elsif block
        # TODO: report on file & line number
        fail 'Block form is not allowed here' unless allow_block
        AliasingProc.new(&block)
      else
        # TODO: support block passing to a class form definition?
        fail 'Class and block form cannot be used together at the moment'
      end
    end
  end
end
