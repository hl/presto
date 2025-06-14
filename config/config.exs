import Config

# Example configuration for Presto Rule Registry
# 
# This shows how to configure custom rule modules for your application.
# Copy this configuration to your host application's config/config.exs
# and modify it to use your own rule modules.

# Simple configuration format - map rule names to modules
config :presto, :rule_registry,
  rules: %{
    # Payroll processing rules
    "time_calculation" => Presto.Examples.PayrollRules,
    "overtime_check" => Presto.Examples.PayrollRules,

    # Compliance checking rules  
    "weekly_compliance" => Presto.Examples.ComplianceRules,

    # Jurisdiction-specific rules
    "spike_break_compliance" => Presto.Examples.CaliforniaSpikeBreakRules
  }

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
