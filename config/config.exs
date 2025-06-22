import Config

# Configure Logger metadata for Presto
# Note: As a library, Presto does not set the global logger level
# The host application should configure logging as needed
config :logger, :console,
  metadata: [:error_type, :error_message, :context, :component, :timestamp, :stacktrace]

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
