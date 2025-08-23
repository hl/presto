import Config

# Configure Logger metadata for Presto
# Note: As a library, Presto does not set the global logger level
# The host application should configure logging as needed
config :logger, :console,
  metadata: [
    # Original metadata keys
    :error_type,
    :error_message,
    :context,
    :component,
    :timestamp,
    :stacktrace,

    # Engine/System metadata
    :engine,
    :engine_id,
    :node,
    :nodes,
    :pid,
    :existing_pid,
    :old_pid,

    # Rules/Facts metadata
    :rule,
    :rule_id,
    :fact_type,
    :fact_count,
    :conditions_count,
    :rule_count,

    # Operations metadata
    :action,
    :action_type,
    :operation,
    :status,
    :duration,
    :duration_ms,

    # Alerts/Metrics metadata
    :alert,
    :alert_id,
    :metric,
    :metric_type,
    :measurements,
    :value,
    :level,
    :threshold_ms,
    :throughput,
    :memory_mb,
    :max_throughput,
    :breaking_point,

    # Networking/Distribution metadata
    :from_node,
    :target_nodes,
    :replication_factor,
    :failed_node,
    :failed_nodes,
    :remaining_nodes,
    :connection_id,
    :consistency,
    :consul_host,
    :port,

    # Debugging/Analysis metadata
    :session_id,
    :trace_level,
    :breakpoint_id,
    :execution_id,
    :hit_count,
    :conflict_id,
    :conflict_type,
    :resolution_id,
    :suggestion_id,
    :suggestions,
    :suggestions_count,
    :issues_found,
    :regression_details,
    :regressions,

    # Configuration metadata
    :config,
    :enabled,
    :interval,
    :priority,
    :complexity,
    :aggregation_type,
    :baseline_id,
    :snapshot_id,
    :rollback_id,
    :replication_id,
    :count,
    :concurrent_users,
    :scenarios,
    :result_count,
    :group_count,

    # Communication metadata
    :channel,
    :channels,
    :email,
    :webhook,
    :message,
    :subscriber,
    :service,
    :namespace,
    :event,
    :type,
    :target,
    :from,
    :to,
    :side,
    :filter,

    # Files/Data metadata
    :file,
    :expected_file,
    :fact_size,
    :metadata,
    :changes,
    :change,

    # General metadata
    :id,
    :name,
    :reason,
    :error,
    :error_module,
    :ex,
    :opts,
    :dry_run,
    :strategy,
    :trend,
    :severity,
    :selected_rule,
    :new_pid,
    :focus_areas,
    :concurrent,
    :usage_percent,
    :overall_status
  ]

# Presto-specific logging configuration
# The host application can override these settings
config :presto, :logging,
  # Enable/disable Presto's internal logging (default: false for libraries)
  # Note: Examples will enable logging via environment variables
  enabled: false,
  # Minimum level for Presto logging when enabled (default: :info)
  level: :info

# Enable logging for examples by default (when running mix directly)
if Mix.env() == :dev and Code.ensure_loaded?(Mix.Project) do
  case Mix.Project.get() do
    Presto.MixProject ->
      # Running examples directly - enable logging
      config :presto, :logging, enabled: true, level: :info

    _ ->
      # Used as dependency - keep disabled
      :ok
  end
end

# Example configuration for Presto Rule Registry
# 
# This shows how to configure custom rule modules for your application.
# Copy this configuration to your host application's config/config.exs
# and modify it to use your own rule modules.

# Simple configuration format - map rule names to modules
# NOTE: Example rule modules have been converted to standalone scripts
# Uncomment and update with your own rule modules that implement Presto.RuleBehaviour
# config :presto, :rule_registry,
#   rules: %{
#     # Payroll processing rules
#     "time_calculation" => MyApp.PayrollRules,
#     "overtime_check" => MyApp.PayrollRules,
#
#     # Compliance checking rules  
#     "weekly_compliance" => MyApp.ComplianceRules,
#
#     # Jurisdiction-specific rules
#     "spike_break_compliance" => MyApp.CaliforniaSpikeBreakRules
#   }

# Alternative advanced configuration format - group rules by module
# config :presto, :rule_registry,
#   rules: [
#     {MyApp.PayrollRules, ["time_calculation", "overtime_check"]},
#     {MyApp.ComplianceRules, ["weekly_compliance", "audit_compliance"]},
#     {MyApp.CustomRules, ["custom_rule_1", "custom_rule_2"]}
#   ]

# For production applications, you should:
# 1. Create your own rule modules that implement Presto.RuleBehaviour
# 2. Replace the example modules above with your custom modules
# 3. Define only the rules your application actually needs
# 4. Consider using environment-specific configurations

# Example of environment-specific configuration:
# if config_env() == :prod do
#   config :presto, :rule_registry,
#     rules: %{
#       "production_rule" => MyApp.ProductionRules
#     }
# end
