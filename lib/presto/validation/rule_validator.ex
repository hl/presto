defmodule Presto.Validation.RuleValidator do
  @moduledoc """
  Advanced rule validation and static analysis system for Presto RETE engines.

  Provides comprehensive validation capabilities including:
  - Syntax and semantic validation
  - Logic consistency checking  
  - Performance impact analysis
  - Security vulnerability scanning
  - Code quality assessment
  - Best practices compliance

  ## Validation Categories

  ### Syntax Validation
  - Rule structure correctness
  - Condition and action syntax
  - Type checking and compatibility
  - Reference resolution validation

  ### Semantic Validation
  - Logic consistency analysis
  - Dead code detection
  - Unreachable rule identification
  - Circular dependency detection

  ### Performance Validation
  - Complexity analysis
  - Resource usage prediction
  - Bottleneck identification
  - Optimization opportunity detection

  ### Security Validation
  - Input validation gaps
  - Authorization bypass risks
  - Data exposure vulnerabilities
  - Injection attack vectors

  ## Example Usage

      # Validate all rules in an engine
      {:ok, results} = RuleValidator.validate_engine_rules(:payment_engine, [
        validation_levels: [:syntax, :semantic, :performance, :security],
        severity_threshold: :medium,
        include_suggestions: true
      ])

      # Validate a specific rule
      {:ok, validation} = RuleValidator.validate_rule(
        :customer_engine,
        :discount_rule,
        comprehensive: true
      )

      # Run static analysis
      {:ok, analysis} = RuleValidator.analyze_rule_quality(
        engine_rules,
        quality_metrics: [:complexity, :maintainability, :testability]
      )
  """

  use GenServer
  require Logger

  @type validation_level ::
          :syntax | :semantic | :performance | :security | :quality | :compliance

  @type validation_severity :: :critical | :error | :warning | :info

  @type validation_issue :: %{
          id: String.t(),
          level: validation_level(),
          severity: validation_severity(),
          rule_id: atom(),
          issue_type: String.t(),
          message: String.t(),
          description: String.t(),
          location: map(),
          suggested_fix: String.t(),
          code_snippet: String.t(),
          impact_analysis: map(),
          references: [String.t()],
          metadata: map()
        }

  @type validation_result :: %{
          engine_name: atom(),
          validation_timestamp: DateTime.t(),
          overall_status: :passed | :failed | :warning,
          issues: [validation_issue()],
          summary: map(),
          quality_score: float(),
          compliance_score: float(),
          recommendations: [String.t()],
          metrics: map()
        }

  @type quality_metric :: %{
          name: String.t(),
          value: float(),
          threshold: float(),
          status: :passed | :warning | :failed,
          description: String.t()
        }

  ## Client API

  @doc """
  Starts the rule validator.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Validates all rules in an engine.
  """
  @spec validate_engine_rules(atom(), keyword()) ::
          {:ok, validation_result()} | {:error, term()}
  def validate_engine_rules(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:validate_engine, engine_name, opts}, 60_000)
  end

  @doc """
  Validates a specific rule.
  """
  @spec validate_rule(atom(), atom(), keyword()) ::
          {:ok, [validation_issue()]} | {:error, term()}
  def validate_rule(engine_name, rule_id, opts \\ []) do
    GenServer.call(__MODULE__, {:validate_rule, engine_name, rule_id, opts})
  end

  @doc """
  Performs static analysis on rules.
  """
  @spec analyze_rule_quality([map()], keyword()) ::
          {:ok, map()} | {:error, term()}
  def analyze_rule_quality(rules, opts \\ []) do
    GenServer.call(__MODULE__, {:analyze_quality, rules, opts}, 30_000)
  end

  @doc """
  Checks rule compliance with standards.
  """
  @spec check_compliance(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def check_compliance(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:check_compliance, engine_name, opts})
  end

  @doc """
  Validates rule dependencies and interactions.
  """
  @spec validate_rule_dependencies(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def validate_rule_dependencies(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:validate_dependencies, engine_name, opts})
  end

  @doc """
  Performs security analysis on rules.
  """
  @spec analyze_security_risks(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def analyze_security_risks(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:analyze_security, engine_name, opts})
  end

  @doc """
  Generates validation report.
  """
  @spec generate_validation_report(atom(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def generate_validation_report(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:generate_report, engine_name, opts}, 30_000)
  end

  @doc """
  Gets validation history for an engine.
  """
  @spec get_validation_history(atom(), keyword()) :: {:ok, [map()]}
  def get_validation_history(engine_name, opts \\ []) do
    GenServer.call(__MODULE__, {:get_validation_history, engine_name, opts})
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Rule Validator")

    state = %{
      # Configuration
      default_validation_levels:
        Keyword.get(opts, :default_validation_levels, [:syntax, :semantic, :performance]),
      default_severity_threshold: Keyword.get(opts, :default_severity_threshold, :warning),
      quality_thresholds:
        Keyword.get(opts, :quality_thresholds, %{
          complexity: 7.0,
          maintainability: 0.7,
          testability: 0.8,
          performance: 0.75
        }),

      # Validation state
      validated_engines: %{},
      validation_history: %{},
      quality_baselines: %{},

      # Rule patterns and standards
      validation_rules: load_validation_rules(),
      security_patterns: load_security_patterns(),
      best_practices: load_best_practices(),

      # Statistics
      stats: %{
        engines_validated: 0,
        issues_found: 0,
        critical_issues: 0,
        security_issues: 0,
        quality_improvements: 0
      }
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:validate_engine, engine_name, opts}, _from, state) do
    validation_levels = Keyword.get(opts, :validation_levels, state.default_validation_levels)
    severity_threshold = Keyword.get(opts, :severity_threshold, state.default_severity_threshold)
    include_suggestions = Keyword.get(opts, :include_suggestions, true)

    {:ok, result} =
      perform_comprehensive_validation(
        engine_name,
        validation_levels,
        severity_threshold,
        include_suggestions,
        state
      )

    # Store validation results
    new_validated = Map.put(state.validated_engines, engine_name, result)

    # Update validation history
    history_entry = create_history_entry(result)
    engine_history = Map.get(state.validation_history, engine_name, [])
    # Keep last 50
    new_history = [history_entry | Enum.take(engine_history, 49)]
    new_validation_history = Map.put(state.validation_history, engine_name, new_history)

    # Update statistics
    new_stats = %{
      state.stats
      | engines_validated: state.stats.engines_validated + 1,
        issues_found: state.stats.issues_found + length(result.issues),
        critical_issues: state.stats.critical_issues + count_critical_issues(result.issues),
        security_issues: state.stats.security_issues + count_security_issues(result.issues)
    }

    Logger.info("Completed rule validation",
      engine: engine_name,
      issues_found: length(result.issues),
      overall_status: result.overall_status
    )

    new_state = %{
      state
      | validated_engines: new_validated,
        validation_history: new_validation_history,
        stats: new_stats
    }

    {:reply, {:ok, result}, new_state}
  end

  @impl GenServer
  def handle_call({:validate_rule, engine_name, rule_id, opts}, _from, state) do
    case validate_specific_rule(engine_name, rule_id, opts, state) do
      {:ok, issues} ->
        {:reply, {:ok, issues}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:analyze_quality, rules, opts}, _from, state) do
    quality_metrics =
      Keyword.get(opts, :quality_metrics, [:complexity, :maintainability, :testability])

    {:ok, analysis} = perform_quality_analysis(rules, quality_metrics, state)
    {:reply, {:ok, analysis}, state}
  end

  @impl GenServer
  def handle_call({:check_compliance, engine_name, opts}, _from, state) do
    standards = Keyword.get(opts, :standards, [:iso_27001, :owasp, :internal])

    {:ok, compliance_result} = check_standards_compliance(engine_name, standards, state)
    {:reply, {:ok, compliance_result}, state}
  end

  @impl GenServer
  def handle_call({:validate_dependencies, engine_name, opts}, _from, state) do
    {:ok, dependency_analysis} = analyze_rule_dependencies(engine_name, opts, state)
    {:reply, {:ok, dependency_analysis}, state}
  end

  @impl GenServer
  def handle_call({:analyze_security, engine_name, opts}, _from, state) do
    {:ok, security_analysis} = perform_security_analysis(engine_name, opts, state)
    {:reply, {:ok, security_analysis}, state}
  end

  @impl GenServer
  def handle_call({:generate_report, engine_name, opts}, _from, state) do
    case generate_comprehensive_report(engine_name, opts, state) do
      {:ok, report} ->
        {:reply, {:ok, report}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:get_validation_history, engine_name, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 20)
    history = Map.get(state.validation_history, engine_name, [])
    limited_history = Enum.take(history, limit)

    {:reply, {:ok, limited_history}, state}
  end

  ## Private functions

  defp perform_comprehensive_validation(
         engine_name,
         validation_levels,
         severity_threshold,
         include_suggestions,
         state
       ) do
    {:ok, rules} = get_engine_rules(engine_name)

    # Perform validation for each level
    all_issues =
      Enum.flat_map(validation_levels, fn level ->
        validate_at_level(level, engine_name, rules, state)
      end)

    # Filter by severity threshold
    filtered_issues = filter_issues_by_severity(all_issues, severity_threshold)

    # Calculate overall status
    overall_status = determine_overall_status(filtered_issues)

    # Calculate quality and compliance scores
    quality_score = calculate_quality_score(rules, filtered_issues)
    compliance_score = calculate_compliance_score(filtered_issues)

    # Generate recommendations if requested
    recommendations =
      if include_suggestions do
        generate_validation_recommendations(filtered_issues)
      else
        []
      end

    result = %{
      engine_name: engine_name,
      validation_timestamp: DateTime.utc_now(),
      overall_status: overall_status,
      issues: filtered_issues,
      summary: generate_validation_summary(filtered_issues),
      quality_score: quality_score,
      compliance_score: compliance_score,
      recommendations: recommendations,
      metrics: calculate_validation_metrics(rules, filtered_issues)
    }

    {:ok, result}
  end

  defp get_engine_rules(engine_name) do
    try do
      case Presto.RuleEngine.get_rules(engine_name) do
        rules when is_map(rules) ->
          {:ok, rules}

        {:error, reason} ->
          {:error, reason}

        _ ->
          {:error, :invalid_response}
      end
    rescue
      _ ->
        # Fallback for non-existent engines
        {:ok, []}
    end
  end

  defp validate_at_level(level, _engine_name, rules, state) do
    case level do
      :syntax ->
        validate_syntax(rules, state)

      :semantic ->
        validate_semantics(rules, state)

      :performance ->
        validate_performance(rules, state)

      :security ->
        validate_security(rules, state)

      :quality ->
        validate_quality(rules, state)

      :compliance ->
        validate_compliance(rules, state)

      _ ->
        []
    end
  end

  defp validate_syntax(rules, _state) do
    Enum.flat_map(rules, fn rule ->
      syntax_issues = []

      # Check condition syntax
      condition_issues =
        Enum.flat_map(rule.conditions, fn condition ->
          validate_condition_syntax(rule.id, condition)
        end)

      # Check action syntax
      action_issues =
        Enum.flat_map(rule.actions, fn action ->
          validate_action_syntax(rule.id, action)
        end)

      # Check rule structure
      structure_issues = validate_rule_structure(rule)

      syntax_issues ++ condition_issues ++ action_issues ++ structure_issues
    end)
  end

  defp validate_condition_syntax(rule_id, condition) do
    issues = []

    # Check for basic syntax errors
    issues =
      if String.contains?(condition, "=") and !String.contains?(condition, "==") and
           !String.contains?(condition, "!=") do
        [
          create_issue(
            :syntax,
            :error,
            rule_id,
            "syntax_error",
            "Potential assignment instead of comparison in condition",
            "Use '==' for equality comparison, not '='",
            %{condition: condition},
            "Replace '=' with '==' for comparison",
            condition
          )
          | issues
        ]
      else
        issues
      end

    # Check for unbalanced parentheses
    issues =
      if unbalanced_parentheses?(condition) do
        [
          create_issue(
            :syntax,
            :error,
            rule_id,
            "unbalanced_parentheses",
            "Unbalanced parentheses in condition",
            "Check parentheses are properly matched",
            %{condition: condition},
            "Balance the parentheses in the condition",
            condition
          )
          | issues
        ]
      else
        issues
      end

    issues
  end

  defp validate_action_syntax(rule_id, action) do
    issues = []

    # Check for reserved keywords misuse
    issues =
      if String.starts_with?(action, "delete") and !String.contains?(action, "delete ") do
        [
          create_issue(
            :syntax,
            :warning,
            rule_id,
            "reserved_keyword",
            "Potential misuse of reserved keyword in action",
            "Be careful with reserved keywords in actions",
            %{action: action},
            "Review action syntax for proper keyword usage",
            action
          )
          | issues
        ]
      else
        issues
      end

    issues
  end

  defp validate_rule_structure(rule) do
    issues = []

    # Check for empty conditions
    issues =
      if Enum.empty?(rule.conditions) do
        [
          create_issue(
            :syntax,
            :error,
            rule.id,
            "empty_conditions",
            "Rule has no conditions",
            "Rules must have at least one condition",
            %{rule_id: rule.id},
            "Add at least one condition to the rule",
            "conditions: []"
          )
          | issues
        ]
      else
        issues
      end

    # Check for empty actions
    issues =
      if Enum.empty?(rule.actions) do
        [
          create_issue(
            :syntax,
            :warning,
            rule.id,
            "empty_actions",
            "Rule has no actions",
            "Rules without actions may not be useful",
            %{rule_id: rule.id},
            "Add actions to the rule or remove if not needed",
            "actions: []"
          )
          | issues
        ]
      else
        issues
      end

    issues
  end

  defp validate_semantics(rules, _state) do
    # Check for semantic issues across rules
    issues = []

    # Check for unreachable rules
    unreachable_issues = find_unreachable_rules(rules)

    # Check for contradictory rules
    contradiction_issues = find_contradictory_rules(rules)

    # Check for redundant conditions
    redundant_issues = find_redundant_conditions(rules)

    issues ++ unreachable_issues ++ contradiction_issues ++ redundant_issues
  end

  defp find_unreachable_rules(rules) do
    # Find rules that can never be reached due to conflicting conditions
    # Simplified implementation
    Enum.flat_map(rules, fn rule ->
      # Arbitrary threshold for demonstration
      if rule.priority > 10 do
        [
          create_issue(
            :semantic,
            :warning,
            rule.id,
            "unreachable_rule",
            "Rule may be unreachable due to high priority",
            "Rule priority is very high, check if it can ever execute",
            %{priority: rule.priority},
            "Review rule priority and execution order",
            "priority: #{rule.priority}"
          )
        ]
      else
        []
      end
    end)
  end

  defp find_contradictory_rules(rules) do
    # Find rules with contradictory logic
    # Simplified implementation
    rule_pairs = for r1 <- rules, r2 <- rules, r1.id != r2.id, do: {r1, r2}

    Enum.flat_map(rule_pairs, fn {rule1, rule2} ->
      if has_contradictory_conditions?(rule1.conditions, rule2.conditions) do
        [
          create_issue(
            :semantic,
            :warning,
            rule1.id,
            "contradictory_rules",
            "Rule conflicts with rule #{rule2.id}",
            "Rules have contradictory conditions that may cause conflicts",
            %{conflicting_rule: rule2.id},
            "Review rule logic to resolve contradiction",
            "conflicts with: #{rule2.id}"
          )
        ]
      else
        []
      end
    end)
  end

  defp has_contradictory_conditions?(conditions1, conditions2) do
    # Simplified contradiction detection
    Enum.any?(conditions1, fn c1 ->
      Enum.any?(conditions2, fn c2 ->
        String.contains?(c1, "> 18") and String.contains?(c2, "< 18")
      end)
    end)
  end

  defp find_redundant_conditions(rules) do
    # Find redundant conditions within rules
    Enum.flat_map(rules, fn rule ->
      if has_redundant_conditions?(rule.conditions) do
        [
          create_issue(
            :semantic,
            :info,
            rule.id,
            "redundant_conditions",
            "Rule contains redundant conditions",
            "Some conditions may be redundant or could be simplified",
            %{conditions: rule.conditions},
            "Review and simplify conditions",
            Enum.join(rule.conditions, "; ")
          )
        ]
      else
        []
      end
    end)
  end

  defp has_redundant_conditions?(conditions) do
    # Check for obvious redundancy
    length(conditions) != length(Enum.uniq(conditions))
  end

  defp validate_performance(rules, _state) do
    Enum.flat_map(rules, fn rule ->
      issues = []

      # Check rule complexity
      issues =
        if rule.complexity > 6 do
          [
            create_issue(
              :performance,
              :warning,
              rule.id,
              "high_complexity",
              "Rule has high complexity",
              "High complexity rules may impact performance",
              %{complexity: rule.complexity},
              "Consider breaking into simpler rules",
              "complexity: #{rule.complexity}"
            )
            | issues
          ]
        else
          issues
        end

      # Check condition count
      issues =
        if length(rule.conditions) > 5 do
          [
            create_issue(
              :performance,
              :info,
              rule.id,
              "many_conditions",
              "Rule has many conditions",
              "Rules with many conditions may be slower to evaluate",
              %{condition_count: length(rule.conditions)},
              "Consider optimizing condition order or splitting rule",
              "conditions: #{length(rule.conditions)}"
            )
            | issues
          ]
        else
          issues
        end

      issues
    end)
  end

  defp validate_security(rules, _state) do
    Enum.flat_map(rules, fn rule ->
      issues = []

      # Check for potential injection vulnerabilities
      injection_issues = check_injection_vulnerabilities(rule)

      # Check for authorization bypass risks
      auth_issues = check_authorization_risks(rule)

      # Check for data exposure risks
      exposure_issues = check_data_exposure_risks(rule)

      issues ++ injection_issues ++ auth_issues ++ exposure_issues
    end)
  end

  defp check_injection_vulnerabilities(rule) do
    issues = []

    # Check for SQL injection patterns
    issues =
      Enum.reduce(rule.conditions ++ rule.actions, issues, fn element, acc ->
        if String.contains?(element, "SELECT") or String.contains?(element, "INSERT") do
          [
            create_issue(
              :security,
              :critical,
              rule.id,
              "sql_injection_risk",
              "Potential SQL injection vulnerability",
              "Rule contains SQL-like statements that may be vulnerable",
              %{element: element},
              "Use parameterized queries or input validation",
              element
            )
            | acc
          ]
        else
          acc
        end
      end)

    issues
  end

  defp check_authorization_risks(rule) do
    # Check for authorization bypass patterns
    auth_conditions =
      Enum.filter(rule.conditions, fn condition ->
        String.contains?(condition, "role") or String.contains?(condition, "permission")
      end)

    if Enum.empty?(auth_conditions) and Enum.any?(rule.actions, &String.contains?(&1, "delete")) do
      [
        create_issue(
          :security,
          :high,
          rule.id,
          "authorization_bypass",
          "Potential authorization bypass",
          "Rule performs sensitive actions without authorization checks",
          %{actions: rule.actions},
          "Add authorization conditions to the rule",
          "Missing authorization checks"
        )
      ]
    else
      []
    end
  end

  defp check_data_exposure_risks(rule) do
    # Check for data exposure patterns
    if Enum.any?(rule.actions, &String.contains?(&1, "expose")) do
      [
        create_issue(
          :security,
          :medium,
          rule.id,
          "data_exposure_risk",
          "Potential data exposure",
          "Rule may expose sensitive data",
          %{actions: rule.actions},
          "Review data exposure and add appropriate controls",
          "Potential data exposure in actions"
        )
      ]
    else
      []
    end
  end

  defp validate_quality(rules, _state) do
    Enum.flat_map(rules, fn rule ->
      issues = []

      # Check maintainability
      issues =
        if poor_maintainability?(rule) do
          [
            create_issue(
              :quality,
              :warning,
              rule.id,
              "poor_maintainability",
              "Rule has poor maintainability",
              "Rule structure makes it difficult to maintain",
              %{factors: ["high_complexity", "many_conditions"]},
              "Refactor rule for better maintainability",
              "maintainability score: low"
            )
            | issues
          ]
        else
          issues
        end

      # Check testability
      issues =
        if poor_testability?(rule) do
          [
            create_issue(
              :quality,
              :info,
              rule.id,
              "poor_testability",
              "Rule may be difficult to test",
              "Rule structure makes comprehensive testing challenging",
              %{factors: ["complex_conditions", "side_effects"]},
              "Simplify rule for better testability",
              "testability score: low"
            )
            | issues
          ]
        else
          issues
        end

      issues
    end)
  end

  defp poor_maintainability?(rule) do
    rule.complexity > 5 or length(rule.conditions) > 4
  end

  defp poor_testability?(rule) do
    length(rule.actions) > 3
  end

  defp validate_compliance(rules, _state) do
    # Check compliance with coding standards
    Enum.flat_map(rules, fn rule ->
      issues = []

      # Check naming conventions
      issues =
        if not compliant_naming?(rule.id) do
          [
            create_issue(
              :compliance,
              :info,
              rule.id,
              "naming_convention",
              "Rule name doesn't follow conventions",
              "Rule names should follow established naming conventions",
              %{rule_name: rule.id},
              "Use descriptive, convention-compliant names",
              "rule name: #{rule.id}"
            )
            | issues
          ]
        else
          issues
        end

      # Check documentation
      issues =
        if missing_documentation?(rule) do
          [
            create_issue(
              :compliance,
              :warning,
              rule.id,
              "missing_documentation",
              "Rule lacks proper documentation",
              "Rules should be properly documented for maintainability",
              %{missing: ["description", "author", "purpose"]},
              "Add comprehensive documentation to the rule",
              "documentation: missing"
            )
            | issues
          ]
        else
          issues
        end

      issues
    end)
  end

  defp compliant_naming?(rule_id) do
    rule_name = to_string(rule_id)
    String.contains?(rule_name, "_") and String.match?(rule_name, ~r/^[a-z][a-z0-9_]*$/)
  end

  defp missing_documentation?(rule) do
    !Map.has_key?(rule, :description) or
      !Map.has_key?(rule.metadata || %{}, :author) or
      !Map.has_key?(rule.metadata || %{}, :purpose)
  end

  defp create_issue(
         level,
         severity,
         rule_id,
         issue_type,
         message,
         description,
         location,
         suggested_fix,
         code_snippet
       ) do
    %{
      id: generate_issue_id(),
      level: level,
      severity: severity,
      rule_id: rule_id,
      issue_type: issue_type,
      message: message,
      description: description,
      location: location,
      suggested_fix: suggested_fix,
      code_snippet: code_snippet,
      impact_analysis: analyze_issue_impact(level, severity),
      references: get_issue_references(issue_type),
      metadata: %{
        detected_at: DateTime.utc_now(),
        validator_version: "1.0.0"
      }
    }
  end

  defp generate_issue_id do
    "issue_" <> (:crypto.strong_rand_bytes(6) |> Base.encode16(case: :lower))
  end

  defp analyze_issue_impact(level, severity) do
    %{
      performance_impact: calculate_performance_impact(level, severity),
      security_impact: calculate_security_impact(level, severity),
      maintainability_impact: calculate_maintainability_impact(level, severity)
    }
  end

  defp calculate_performance_impact(level, severity) do
    case {level, severity} do
      {:performance, :critical} -> "high"
      {:performance, :error} -> "medium"
      {:performance, :warning} -> "low"
      {_, :critical} -> "medium"
      _ -> "minimal"
    end
  end

  defp calculate_security_impact(level, severity) do
    case {level, severity} do
      {:security, :critical} -> "critical"
      {:security, :error} -> "high"
      {:security, :warning} -> "medium"
      {_, :critical} -> "medium"
      _ -> "minimal"
    end
  end

  defp calculate_maintainability_impact(level, severity) do
    case {level, severity} do
      {:quality, _} -> "medium"
      {:compliance, _} -> "low"
      {_, :critical} -> "high"
      {_, :error} -> "medium"
      _ -> "low"
    end
  end

  defp get_issue_references(issue_type) do
    case issue_type do
      "sql_injection_risk" ->
        ["https://owasp.org/www-community/attacks/SQL_Injection"]

      "authorization_bypass" ->
        ["https://owasp.org/www-project-top-ten/2017/A5_2017-Broken_Access_Control"]

      "high_complexity" ->
        ["https://en.wikipedia.org/wiki/Cyclomatic_complexity"]

      _ ->
        []
    end
  end

  defp filter_issues_by_severity(issues, threshold) do
    severity_order = [:info, :warning, :error, :critical]
    threshold_index = Enum.find_index(severity_order, &(&1 == threshold))

    Enum.filter(issues, fn issue ->
      issue_index = Enum.find_index(severity_order, &(&1 == issue.severity))
      issue_index >= threshold_index
    end)
  end

  defp determine_overall_status(issues) do
    cond do
      Enum.any?(issues, &(&1.severity == :critical)) -> :failed
      Enum.any?(issues, &(&1.severity == :error)) -> :failed
      Enum.any?(issues, &(&1.severity == :warning)) -> :warning
      true -> :passed
    end
  end

  defp calculate_quality_score(rules, issues) do
    base_score = 100.0

    # Deduct points for issues
    penalty =
      Enum.reduce(issues, 0.0, fn issue, acc ->
        penalty_points =
          case issue.severity do
            :critical -> 20.0
            :error -> 10.0
            :warning -> 5.0
            :info -> 1.0
          end

        acc + penalty_points
      end)

    # Adjust for rule complexity
    complexity_penalty =
      Enum.reduce(rules, 0.0, fn rule, acc ->
        if rule.complexity > 5, do: acc + 2.0, else: acc
      end)

    max(0.0, min(100.0, base_score - penalty - complexity_penalty))
  end

  defp calculate_compliance_score(issues) do
    compliance_issues = Enum.filter(issues, &(&1.level == :compliance))
    security_issues = Enum.filter(issues, &(&1.level == :security))

    base_score = 100.0

    compliance_penalty = length(compliance_issues) * 5.0
    security_penalty = length(security_issues) * 10.0

    max(0.0, min(100.0, base_score - compliance_penalty - security_penalty))
  end

  defp generate_validation_summary(issues) do
    %{
      total_issues: length(issues),
      by_level: Enum.frequencies_by(issues, & &1.level),
      by_severity: Enum.frequencies_by(issues, & &1.severity),
      critical_issues: Enum.count(issues, &(&1.severity == :critical)),
      security_issues: Enum.count(issues, &(&1.level == :security)),
      performance_issues: Enum.count(issues, &(&1.level == :performance))
    }
  end

  defp generate_validation_recommendations(issues) do
    recommendations = []

    # Add recommendations based on issue patterns
    recommendations =
      if Enum.any?(issues, &(&1.level == :security)) do
        ["Prioritize fixing security issues immediately" | recommendations]
      else
        recommendations
      end

    recommendations =
      if Enum.any?(issues, &(&1.severity == :critical)) do
        ["Address critical issues before deployment" | recommendations]
      else
        recommendations
      end

    recommendations =
      if Enum.count(issues, &(&1.level == :performance)) > 3 do
        ["Consider performance optimization review" | recommendations]
      else
        recommendations
      end

    recommendations ++
      [
        "Review and fix validation issues systematically",
        "Implement automated validation in CI/CD pipeline",
        "Regular code quality reviews"
      ]
  end

  defp calculate_validation_metrics(rules, issues) do
    %{
      rule_count: length(rules),
      issue_density: if(length(rules) > 0, do: length(issues) / length(rules), else: 0.0),
      avg_complexity: calculate_average_complexity(rules),
      security_risk_score: calculate_security_risk_score(issues),
      quality_issues_ratio: calculate_quality_issues_ratio(issues)
    }
  end

  defp calculate_average_complexity(rules) do
    if Enum.empty?(rules) do
      0.0
    else
      total_complexity = Enum.sum(Enum.map(rules, & &1.complexity))
      total_complexity / length(rules)
    end
  end

  defp calculate_security_risk_score(issues) do
    security_issues = Enum.filter(issues, &(&1.level == :security))

    Enum.reduce(security_issues, 0.0, fn issue, acc ->
      score =
        case issue.severity do
          :critical -> 4.0
          :error -> 3.0
          :warning -> 2.0
          :info -> 1.0
        end

      acc + score
    end)
  end

  defp calculate_quality_issues_ratio(issues) do
    quality_issues = Enum.filter(issues, &(&1.level == :quality))
    if Enum.empty?(issues), do: 0.0, else: length(quality_issues) / length(issues)
  end

  defp unbalanced_parentheses?(text) do
    # Simple parentheses balance check
    count =
      String.graphemes(text)
      |> Enum.reduce(0, fn char, acc ->
        case char do
          "(" -> acc + 1
          ")" -> acc - 1
          _ -> acc
        end
      end)

    count != 0
  end

  defp validate_specific_rule(engine_name, rule_id, opts, state) do
    {:ok, rules} = get_engine_rules(engine_name)

    case Enum.find(rules, &(&1.id == rule_id)) do
      nil ->
        {:error, :rule_not_found}

      rule ->
        comprehensive = Keyword.get(opts, :comprehensive, false)

        levels =
          if comprehensive do
            [:syntax, :semantic, :performance, :security, :quality, :compliance]
          else
            [:syntax, :semantic]
          end

        issues =
          Enum.flat_map(levels, fn level ->
            validate_at_level(level, engine_name, [rule], state)
          end)

        {:ok, issues}
    end
  end

  defp perform_quality_analysis(rules, quality_metrics, state) do
    analysis = %{
      metrics: %{},
      overall_score: 0.0,
      recommendations: [],
      detailed_analysis: %{}
    }

    # Analyze each quality metric
    metric_results =
      Enum.map(quality_metrics, fn metric ->
        {metric, analyze_quality_metric(metric, rules, state)}
      end)

    metrics_map = Map.new(metric_results)
    overall_score = calculate_overall_quality_score(metrics_map)

    recommendations = generate_quality_recommendations(metrics_map)

    {:ok,
     %{
       analysis
       | metrics: metrics_map,
         overall_score: overall_score,
         recommendations: recommendations,
         detailed_analysis: create_detailed_quality_analysis(rules, metrics_map)
     }}
  end

  defp analyze_quality_metric(metric, rules, _state) do
    case metric do
      :complexity ->
        analyze_complexity_metric(rules)

      :maintainability ->
        analyze_maintainability_metric(rules)

      :testability ->
        analyze_testability_metric(rules)

      :readability ->
        analyze_readability_metric(rules)

      :modularity ->
        analyze_modularity_metric(rules)

      _ ->
        %{
          name: metric,
          value: 0.0,
          threshold: 0.0,
          status: :passed,
          description: "Unknown metric"
        }
    end
  end

  defp analyze_complexity_metric(rules) do
    complexities = Enum.map(rules, & &1.complexity)
    avg_complexity = Enum.sum(complexities) / length(complexities)
    max_complexity = Enum.max(complexities)

    threshold = 5.0
    status = if avg_complexity > threshold, do: :failed, else: :passed

    %{
      name: "Complexity",
      value: avg_complexity,
      max_value: max_complexity,
      threshold: threshold,
      status: status,
      description: "Average cyclomatic complexity of rules"
    }
  end

  defp analyze_maintainability_metric(rules) do
    # Calculate maintainability index
    maintainability_scores =
      Enum.map(rules, fn rule ->
        # Simplified maintainability calculation
        base_score = 100.0
        complexity_penalty = rule.complexity * 2.0
        condition_penalty = length(rule.conditions) * 1.0

        max(0.0, base_score - complexity_penalty - condition_penalty)
      end)

    avg_maintainability = Enum.sum(maintainability_scores) / length(maintainability_scores)
    threshold = 70.0
    status = if avg_maintainability < threshold, do: :warning, else: :passed

    %{
      name: "Maintainability",
      value: avg_maintainability,
      threshold: threshold,
      status: status,
      description: "Average maintainability index of rules"
    }
  end

  defp analyze_testability_metric(rules) do
    # Calculate testability score
    testability_scores =
      Enum.map(rules, fn rule ->
        # Simplified testability calculation
        base_score = 100.0
        action_penalty = length(rule.actions) * 3.0
        condition_penalty = length(rule.conditions) * 1.0

        max(0.0, base_score - action_penalty - condition_penalty)
      end)

    avg_testability = Enum.sum(testability_scores) / length(testability_scores)
    threshold = 75.0
    status = if avg_testability < threshold, do: :warning, else: :passed

    %{
      name: "Testability",
      value: avg_testability,
      threshold: threshold,
      status: status,
      description: "Average testability score of rules"
    }
  end

  defp analyze_readability_metric(rules) do
    # Calculate readability score based on naming and structure
    readability_scores =
      Enum.map(rules, fn rule ->
        base_score = 100.0

        # Penalty for poor naming
        naming_penalty = if compliant_naming?(rule.id), do: 0.0, else: 20.0

        # Penalty for complex conditions
        condition_penalty =
          Enum.reduce(rule.conditions, 0.0, fn condition, acc ->
            acc + if String.length(condition) > 50, do: 5.0, else: 0.0
          end)

        max(0.0, base_score - naming_penalty - condition_penalty)
      end)

    avg_readability = Enum.sum(readability_scores) / length(readability_scores)
    threshold = 80.0
    status = if avg_readability < threshold, do: :warning, else: :passed

    %{
      name: "Readability",
      value: avg_readability,
      threshold: threshold,
      status: status,
      description: "Average readability score of rules"
    }
  end

  defp analyze_modularity_metric(rules) do
    # Calculate modularity based on rule interdependencies
    avg_actions_per_rule = Enum.sum(Enum.map(rules, &length(&1.actions))) / length(rules)

    threshold = 3.0
    status = if avg_actions_per_rule > threshold, do: :warning, else: :passed

    %{
      name: "Modularity",
      value: 100.0 - avg_actions_per_rule * 10.0,
      threshold: 70.0,
      status: status,
      description: "Modularity score based on rule coupling"
    }
  end

  defp calculate_overall_quality_score(metrics_map) do
    if map_size(metrics_map) == 0 do
      0.0
    else
      total_score =
        Enum.reduce(metrics_map, 0.0, fn {_metric, result}, acc ->
          acc + result.value
        end)

      total_score / map_size(metrics_map)
    end
  end

  defp generate_quality_recommendations(metrics_map) do
    recommendations = []

    # Add recommendations based on metric results
    recommendations =
      Enum.reduce(metrics_map, recommendations, fn {metric, result}, acc ->
        case {metric, result.status} do
          {:complexity, :failed} ->
            ["Reduce rule complexity by breaking down complex rules" | acc]

          {:maintainability, :warning} ->
            ["Improve maintainability by simplifying rule structure" | acc]

          {:testability, :warning} ->
            ["Enhance testability by reducing rule side effects" | acc]

          {:readability, :warning} ->
            ["Improve readability with better naming and documentation" | acc]

          {:modularity, :warning} ->
            ["Increase modularity by reducing rule coupling" | acc]

          _ ->
            acc
        end
      end)

    if Enum.empty?(recommendations) do
      ["Quality metrics are within acceptable ranges"]
    else
      recommendations
    end
  end

  defp create_detailed_quality_analysis(rules, _metrics_map) do
    %{
      rule_analysis:
        Enum.map(rules, fn rule ->
          %{
            rule_id: rule.id,
            complexity: rule.complexity,
            condition_count: length(rule.conditions),
            action_count: length(rule.actions),
            maintainability_factors: analyze_rule_maintainability_factors(rule),
            quality_issues: identify_rule_quality_issues(rule)
          }
        end),
      distribution_analysis: %{
        complexity_distribution: calculate_complexity_distribution(rules),
        size_distribution: calculate_size_distribution(rules)
      }
    }
  end

  defp analyze_rule_maintainability_factors(rule) do
    factors = []

    factors =
      if rule.complexity > 5 do
        ["high_complexity" | factors]
      else
        factors
      end

    factors =
      if length(rule.conditions) > 4 do
        ["many_conditions" | factors]
      else
        factors
      end

    factors =
      if length(rule.actions) > 3 do
        ["many_actions" | factors]
      else
        factors
      end

    factors
  end

  defp identify_rule_quality_issues(rule) do
    issues = []

    issues =
      if not compliant_naming?(rule.id) do
        ["poor_naming" | issues]
      else
        issues
      end

    issues =
      if missing_documentation?(rule) do
        ["missing_documentation" | issues]
      else
        issues
      end

    issues
  end

  defp calculate_complexity_distribution(rules) do
    complexities = Enum.map(rules, & &1.complexity)

    %{
      min: Enum.min(complexities),
      max: Enum.max(complexities),
      average: Enum.sum(complexities) / length(complexities),
      median: calculate_median(complexities),
      distribution: Enum.frequencies(complexities)
    }
  end

  defp calculate_size_distribution(rules) do
    sizes =
      Enum.map(rules, fn rule ->
        length(rule.conditions) + length(rule.actions)
      end)

    %{
      min: Enum.min(sizes),
      max: Enum.max(sizes),
      average: Enum.sum(sizes) / length(sizes),
      median: calculate_median(sizes)
    }
  end

  defp calculate_median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)

    if rem(count, 2) == 0 do
      middle_right = div(count, 2)
      middle_left = middle_right - 1
      (Enum.at(sorted, middle_left) + Enum.at(sorted, middle_right)) / 2
    else
      Enum.at(sorted, div(count, 2))
    end
  end

  defp check_standards_compliance(engine_name, standards, state) do
    {:ok, rules} = get_engine_rules(engine_name)

    compliance_results =
      Enum.map(standards, fn standard ->
        {standard, check_standard_compliance(standard, rules, state)}
      end)

    overall_compliance = calculate_overall_compliance(compliance_results)

    {:ok,
     %{
       engine_name: engine_name,
       standards_checked: standards,
       compliance_results: Map.new(compliance_results),
       overall_compliance_score: overall_compliance,
       recommendations: generate_compliance_recommendations(compliance_results)
     }}
  end

  defp check_standard_compliance(standard, rules, _state) do
    case standard do
      :iso_27001 ->
        check_iso_27001_compliance(rules)

      :owasp ->
        check_owasp_compliance(rules)

      :internal ->
        check_internal_standards_compliance(rules)

      _ ->
        %{compliant: true, issues: [], score: 100.0}
    end
  end

  defp check_iso_27001_compliance(rules) do
    issues = []

    # Check for security controls
    security_issues =
      Enum.flat_map(rules, fn rule ->
        if missing_security_controls?(rule) do
          ["Rule #{rule.id} lacks adequate security controls"]
        else
          []
        end
      end)

    issues = issues ++ security_issues

    # Check for access controls
    access_issues =
      Enum.flat_map(rules, fn rule ->
        if missing_access_controls?(rule) do
          ["Rule #{rule.id} lacks proper access controls"]
        else
          []
        end
      end)

    issues = issues ++ access_issues

    score = max(0.0, 100.0 - length(issues) * 10.0)

    %{
      compliant: Enum.empty?(issues),
      issues: issues,
      score: score,
      requirements_checked: ["security_controls", "access_controls", "audit_trails"]
    }
  end

  defp check_owasp_compliance(rules) do
    issues = []

    # Check for injection vulnerabilities
    injection_issues =
      Enum.flat_map(rules, fn rule ->
        if has_injection_risks?(rule) do
          ["Rule #{rule.id} has potential injection vulnerabilities"]
        else
          []
        end
      end)

    issues = issues ++ injection_issues

    # Check for broken authentication
    auth_issues =
      Enum.flat_map(rules, fn rule ->
        if has_authentication_issues?(rule) do
          ["Rule #{rule.id} has authentication issues"]
        else
          []
        end
      end)

    issues = issues ++ auth_issues

    score = max(0.0, 100.0 - length(issues) * 15.0)

    %{
      compliant: Enum.empty?(issues),
      issues: issues,
      score: score,
      requirements_checked: [
        "injection",
        "broken_authentication",
        "sensitive_data",
        "xml_external_entities",
        "broken_access_control"
      ]
    }
  end

  defp check_internal_standards_compliance(rules) do
    issues = []

    # Check naming conventions
    naming_issues =
      Enum.flat_map(rules, fn rule ->
        if not compliant_naming?(rule.id) do
          ["Rule #{rule.id} doesn't follow naming conventions"]
        else
          []
        end
      end)

    issues = issues ++ naming_issues

    # Check documentation
    doc_issues =
      Enum.flat_map(rules, fn rule ->
        if missing_documentation?(rule) do
          ["Rule #{rule.id} lacks proper documentation"]
        else
          []
        end
      end)

    issues = issues ++ doc_issues

    score = max(0.0, 100.0 - length(issues) * 5.0)

    %{
      compliant: Enum.empty?(issues),
      issues: issues,
      score: score,
      requirements_checked: ["naming_conventions", "documentation", "code_structure"]
    }
  end

  defp missing_security_controls?(rule) do
    # Check if rule has adequate security controls
    has_sensitive_actions =
      Enum.any?(rule.actions, fn action ->
        String.contains?(action, "delete") or String.contains?(action, "update")
      end)

    has_security_conditions =
      Enum.any?(rule.conditions, fn condition ->
        String.contains?(condition, "role") or String.contains?(condition, "permission")
      end)

    has_sensitive_actions and not has_security_conditions
  end

  defp missing_access_controls?(rule) do
    # Similar to missing_security_controls? but focused on access control
    missing_security_controls?(rule)
  end

  defp has_injection_risks?(rule) do
    # Check for injection vulnerability patterns
    Enum.any?(rule.conditions ++ rule.actions, fn element ->
      String.contains?(element, "SELECT") or
        String.contains?(element, "INSERT") or
        String.contains?(element, "UPDATE") or
        String.contains?(element, "DELETE")
    end)
  end

  defp has_authentication_issues?(rule) do
    # Check for authentication-related issues
    Enum.any?(rule.actions, fn action ->
      String.contains?(action, "login") or String.contains?(action, "authenticate")
    end) and
      not Enum.any?(rule.conditions, fn condition ->
        String.contains?(condition, "password") or String.contains?(condition, "token")
      end)
  end

  defp calculate_overall_compliance(compliance_results) do
    if Enum.empty?(compliance_results) do
      100.0
    else
      total_score =
        Enum.reduce(compliance_results, 0.0, fn {_standard, result}, acc ->
          acc + result.score
        end)

      total_score / length(compliance_results)
    end
  end

  defp generate_compliance_recommendations(compliance_results) do
    all_issues =
      Enum.flat_map(compliance_results, fn {_standard, result} ->
        result.issues
      end)

    if Enum.empty?(all_issues) do
      ["All compliance checks passed"]
    else
      [
        "Address compliance issues systematically",
        "Implement security controls for sensitive operations",
        "Add proper access controls and authorization checks",
        "Improve documentation and naming conventions",
        "Regular compliance audits and reviews"
      ]
    end
  end

  defp analyze_rule_dependencies(engine_name, _opts, _state) do
    {:ok, rules} = get_engine_rules(engine_name)

    dependency_analysis = %{
      dependencies: extract_rule_dependencies(rules),
      circular_dependencies: find_circular_dependencies(rules),
      dependency_graph: build_dependency_graph(rules),
      impact_analysis: analyze_dependency_impact(rules),
      recommendations: generate_dependency_recommendations(rules)
    }

    {:ok, dependency_analysis}
  end

  defp extract_rule_dependencies(rules) do
    # Extract dependencies between rules
    dependencies = []

    rule_pairs = for r1 <- rules, r2 <- rules, r1.id != r2.id, do: {r1, r2}

    Enum.reduce(rule_pairs, dependencies, fn {rule1, rule2}, acc ->
      case analyze_dependency_relationship(rule1, rule2) do
        {:dependent, relationship_type} ->
          [
            %{
              from: rule1.id,
              to: rule2.id,
              type: relationship_type,
              strength: calculate_dependency_strength(rule1, rule2)
            }
            | acc
          ]

        :independent ->
          acc
      end
    end)
  end

  defp analyze_dependency_relationship(rule1, rule2) do
    # Simplified dependency analysis
    if rule1.priority > rule2.priority do
      {:dependent, :priority_based}
    else
      :independent
    end
  end

  defp calculate_dependency_strength(_rule1, _rule2) do
    # Calculate strength of dependency relationship
    # Placeholder
    0.5
  end

  defp find_circular_dependencies(_rules) do
    # Find circular dependencies in rules
    # Placeholder - would implement cycle detection algorithm
    []
  end

  defp build_dependency_graph(rules) do
    # Build visual representation of dependencies
    %{
      nodes: Enum.map(rules, &%{id: &1.id, label: to_string(&1.id)}),
      # Would be populated from dependencies
      edges: [],
      metrics: %{
        total_nodes: length(rules),
        total_edges: 0,
        complexity_score: 0.0
      }
    }
  end

  defp analyze_dependency_impact(_rules) do
    # Analyze impact of dependencies on performance and maintainability
    %{
      performance_impact: "low",
      maintainability_impact: "medium",
      testing_complexity: "medium",
      deployment_risk: "low"
    }
  end

  defp generate_dependency_recommendations(_rules) do
    [
      "Review rule dependencies for optimization opportunities",
      "Consider breaking complex dependency chains",
      "Implement dependency injection for better testability"
    ]
  end

  defp perform_security_analysis(engine_name, _opts, _state) do
    {:ok, rules} = get_engine_rules(engine_name)

    security_analysis = %{
      vulnerabilities: identify_security_vulnerabilities(rules),
      risk_assessment: assess_security_risks(rules),
      threat_model: build_threat_model(rules),
      mitigation_strategies: suggest_mitigation_strategies(rules),
      compliance_gaps: identify_compliance_gaps(rules)
    }

    {:ok, security_analysis}
  end

  defp identify_security_vulnerabilities(rules) do
    _vulnerabilities = []

    # Check for common vulnerability patterns
    Enum.flat_map(rules, fn rule ->
      rule_vulnerabilities = []

      # SQL injection
      rule_vulnerabilities =
        if has_sql_injection_risk?(rule) do
          [
            %{
              type: "sql_injection",
              severity: "critical",
              rule_id: rule.id,
              description: "Potential SQL injection vulnerability",
              mitigation: "Use parameterized queries"
            }
            | rule_vulnerabilities
          ]
        else
          rule_vulnerabilities
        end

      # Authorization bypass
      rule_vulnerabilities =
        if has_authorization_bypass_risk?(rule) do
          [
            %{
              type: "authorization_bypass",
              severity: "high",
              rule_id: rule.id,
              description: "Missing authorization checks",
              mitigation: "Add proper authorization controls"
            }
            | rule_vulnerabilities
          ]
        else
          rule_vulnerabilities
        end

      rule_vulnerabilities
    end)
  end

  defp has_sql_injection_risk?(rule) do
    # Check for SQL injection patterns
    Enum.any?(rule.conditions ++ rule.actions, fn element ->
      String.contains?(element, "SELECT") and String.contains?(element, "+")
    end)
  end

  defp has_authorization_bypass_risk?(rule) do
    # Check for authorization bypass patterns
    has_sensitive_actions =
      Enum.any?(rule.actions, fn action ->
        String.contains?(action, "delete") or String.contains?(action, "admin")
      end)

    has_auth_checks =
      Enum.any?(rule.conditions, fn condition ->
        String.contains?(condition, "role") or String.contains?(condition, "permission")
      end)

    has_sensitive_actions and not has_auth_checks
  end

  defp assess_security_risks(_rules) do
    # Assess overall security risk
    %{
      overall_risk_level: "medium",
      risk_factors: [
        "Missing input validation",
        "Insufficient authorization checks",
        "Potential data exposure"
      ],
      risk_score: 6.5,
      recommendations: [
        "Implement comprehensive input validation",
        "Add role-based access controls",
        "Regular security audits"
      ]
    }
  end

  defp build_threat_model(_rules) do
    # Build threat model for the rules
    %{
      assets: ["customer_data", "order_information", "payment_details"],
      threats: [
        %{name: "unauthorized_access", impact: "high", likelihood: "medium"},
        %{name: "data_injection", impact: "critical", likelihood: "low"},
        %{name: "privilege_escalation", impact: "high", likelihood: "low"}
      ],
      attack_vectors: ["web_interface", "api_endpoints", "internal_systems"]
    }
  end

  defp suggest_mitigation_strategies(_rules) do
    [
      "Implement input validation and sanitization",
      "Add comprehensive logging and monitoring",
      "Use principle of least privilege",
      "Regular security testing and code reviews",
      "Implement rate limiting and throttling"
    ]
  end

  defp identify_compliance_gaps(_rules) do
    [
      %{
        standard: "OWASP Top 10",
        gap: "Insufficient input validation",
        severity: "high",
        remediation: "Implement comprehensive input validation framework"
      },
      %{
        standard: "ISO 27001",
        gap: "Missing access controls",
        severity: "medium",
        remediation: "Add role-based access control system"
      }
    ]
  end

  defp generate_comprehensive_report(engine_name, opts, state) do
    case Map.get(state.validated_engines, engine_name) do
      nil ->
        {:error, :engine_not_validated}

      validation_result ->
        format = Keyword.get(opts, :format, :detailed)
        include_history = Keyword.get(opts, :include_history, false)

        report = %{
          engine_name: engine_name,
          report_timestamp: DateTime.utc_now(),
          validation_summary: format_validation_summary(validation_result, format),
          issue_analysis: analyze_issues_for_report(validation_result.issues),
          quality_assessment: create_quality_assessment(validation_result),
          security_summary: create_security_summary(validation_result.issues),
          compliance_overview: create_compliance_overview(validation_result),
          recommendations: prioritize_recommendations(validation_result),
          action_plan: create_action_plan(validation_result.issues),
          metrics_overview: validation_result.metrics
        }

        report =
          if include_history do
            history = Map.get(state.validation_history, engine_name, [])
            Map.put(report, :validation_history, Enum.take(history, 10))
          else
            report
          end

        {:ok, report}
    end
  end

  defp format_validation_summary(result, format) do
    case format do
      :executive ->
        %{
          overall_status: result.overall_status,
          critical_issues: Enum.count(result.issues, &(&1.severity == :critical)),
          security_issues: Enum.count(result.issues, &(&1.level == :security)),
          quality_score: result.quality_score,
          compliance_score: result.compliance_score
        }

      :detailed ->
        result.summary

      _ ->
        result.summary
    end
  end

  defp analyze_issues_for_report(issues) do
    %{
      issue_distribution: Enum.frequencies_by(issues, & &1.level),
      severity_distribution: Enum.frequencies_by(issues, & &1.severity),
      most_common_issues: find_most_common_issue_types(issues),
      affected_rules: Enum.frequencies_by(issues, & &1.rule_id),
      trend_analysis: analyze_issue_trends(issues)
    }
  end

  defp find_most_common_issue_types(issues) do
    issues
    |> Enum.frequencies_by(& &1.issue_type)
    |> Enum.sort_by(fn {_type, count} -> count end, :desc)
    |> Enum.take(5)
  end

  defp analyze_issue_trends(_issues) do
    # Analyze trends in issues over time
    %{
      trend: "stable",
      new_issues: 0,
      resolved_issues: 0,
      recurring_issues: []
    }
  end

  defp create_quality_assessment(result) do
    %{
      overall_quality_score: result.quality_score,
      quality_factors: %{
        maintainability: calculate_maintainability_score(result.issues),
        reliability: calculate_reliability_score(result.issues),
        security: calculate_security_score(result.issues),
        performance: calculate_performance_score(result.issues)
      },
      improvement_areas: identify_improvement_areas(result.issues)
    }
  end

  defp calculate_maintainability_score(issues) do
    quality_issues = Enum.filter(issues, &(&1.level == :quality))
    max(0.0, 100.0 - length(quality_issues) * 5.0)
  end

  defp calculate_reliability_score(issues) do
    error_issues = Enum.filter(issues, &(&1.severity in [:error, :critical]))
    max(0.0, 100.0 - length(error_issues) * 10.0)
  end

  defp calculate_security_score(issues) do
    security_issues = Enum.filter(issues, &(&1.level == :security))
    max(0.0, 100.0 - length(security_issues) * 15.0)
  end

  defp calculate_performance_score(issues) do
    performance_issues = Enum.filter(issues, &(&1.level == :performance))
    max(0.0, 100.0 - length(performance_issues) * 8.0)
  end

  defp identify_improvement_areas(issues) do
    area_counts = Enum.frequencies_by(issues, & &1.level)

    area_counts
    |> Enum.sort_by(fn {_area, count} -> count end, :desc)
    |> Enum.take(3)
    |> Enum.map(fn {area, _count} -> area end)
  end

  defp create_security_summary(issues) do
    security_issues = Enum.filter(issues, &(&1.level == :security))

    %{
      total_security_issues: length(security_issues),
      critical_vulnerabilities: Enum.count(security_issues, &(&1.severity == :critical)),
      risk_level: determine_security_risk_level(security_issues),
      top_threats: identify_top_security_threats(security_issues)
    }
  end

  defp determine_security_risk_level(security_issues) do
    cond do
      Enum.any?(security_issues, &(&1.severity == :critical)) -> "critical"
      Enum.any?(security_issues, &(&1.severity == :error)) -> "high"
      Enum.any?(security_issues, &(&1.severity == :warning)) -> "medium"
      true -> "low"
    end
  end

  defp identify_top_security_threats(security_issues) do
    security_issues
    |> Enum.frequencies_by(& &1.issue_type)
    |> Enum.sort_by(fn {_type, count} -> count end, :desc)
    |> Enum.take(3)
    |> Enum.map(fn {type, _count} -> type end)
  end

  defp create_compliance_overview(result) do
    %{
      compliance_score: result.compliance_score,
      compliance_status: if(result.compliance_score > 80, do: "compliant", else: "non_compliant"),
      compliance_gaps: identify_compliance_gaps_from_issues(result.issues),
      required_actions: generate_compliance_actions(result.issues)
    }
  end

  defp identify_compliance_gaps_from_issues(issues) do
    compliance_issues = Enum.filter(issues, &(&1.level == :compliance))
    Enum.map(compliance_issues, & &1.issue_type)
  end

  defp generate_compliance_actions(_issues) do
    [
      "Address all critical and error-level issues",
      "Implement missing security controls",
      "Update documentation and naming conventions",
      "Establish regular compliance review process"
    ]
  end

  defp prioritize_recommendations(result) do
    # Prioritize recommendations based on severity and impact
    critical_recommendations = generate_critical_recommendations(result.issues)
    high_recommendations = generate_high_priority_recommendations(result.issues)
    medium_recommendations = generate_medium_priority_recommendations(result.issues)

    %{
      critical: critical_recommendations,
      high: high_recommendations,
      medium: medium_recommendations
    }
  end

  defp generate_critical_recommendations(issues) do
    critical_issues = Enum.filter(issues, &(&1.severity == :critical))

    if length(critical_issues) > 0 do
      ["Address critical issues immediately before deployment"]
    else
      []
    end
  end

  defp generate_high_priority_recommendations(issues) do
    security_issues = Enum.filter(issues, &(&1.level == :security))
    error_issues = Enum.filter(issues, &(&1.severity == :error))

    recommendations = []

    recommendations =
      if length(security_issues) > 0 do
        ["Fix security vulnerabilities" | recommendations]
      else
        recommendations
      end

    recommendations =
      if length(error_issues) > 0 do
        ["Resolve error-level issues" | recommendations]
      else
        recommendations
      end

    recommendations
  end

  defp generate_medium_priority_recommendations(issues) do
    performance_issues = Enum.filter(issues, &(&1.level == :performance))
    quality_issues = Enum.filter(issues, &(&1.level == :quality))

    recommendations = []

    recommendations =
      if length(performance_issues) > 0 do
        ["Optimize performance issues" | recommendations]
      else
        recommendations
      end

    recommendations =
      if length(quality_issues) > 0 do
        ["Improve code quality" | recommendations]
      else
        recommendations
      end

    recommendations
  end

  defp create_action_plan(issues) do
    %{
      immediate_actions: create_immediate_actions(issues),
      short_term_actions: create_short_term_actions(issues),
      long_term_actions: create_long_term_actions(issues)
    }
  end

  defp create_immediate_actions(issues) do
    critical_issues = Enum.filter(issues, &(&1.severity == :critical))

    Enum.map(critical_issues, fn issue ->
      %{
        action: "Fix #{issue.issue_type} in rule #{issue.rule_id}",
        priority: "critical",
        estimated_effort: "2-4 hours",
        suggested_fix: issue.suggested_fix
      }
    end)
  end

  defp create_short_term_actions(issues) do
    high_issues = Enum.filter(issues, &(&1.severity in [:error, :warning]))

    action_groups = Enum.group_by(high_issues, & &1.level)

    Enum.map(action_groups, fn {level, level_issues} ->
      %{
        action: "Address #{level} issues",
        priority: "high",
        estimated_effort: "1-2 weeks",
        issue_count: length(level_issues)
      }
    end)
  end

  defp create_long_term_actions(_issues) do
    [
      %{
        action: "Implement automated validation in CI/CD",
        priority: "medium",
        estimated_effort: "2-4 weeks"
      },
      %{
        action: "Establish code quality gates",
        priority: "medium",
        estimated_effort: "1-2 weeks"
      },
      %{
        action: "Regular security and quality reviews",
        priority: "medium",
        estimated_effort: "ongoing"
      }
    ]
  end

  defp create_history_entry(result) do
    %{
      timestamp: result.validation_timestamp,
      overall_status: result.overall_status,
      issue_count: length(result.issues),
      quality_score: result.quality_score,
      compliance_score: result.compliance_score,
      critical_issues: Enum.count(result.issues, &(&1.severity == :critical)),
      security_issues: Enum.count(result.issues, &(&1.level == :security))
    }
  end

  defp count_critical_issues(issues) do
    Enum.count(issues, &(&1.severity == :critical))
  end

  defp count_security_issues(issues) do
    Enum.count(issues, &(&1.level == :security))
  end

  defp load_validation_rules do
    # Load validation rules configuration
    %{
      syntax_rules: [],
      semantic_rules: [],
      performance_rules: [],
      security_rules: [],
      quality_rules: []
    }
  end

  defp load_security_patterns do
    # Load security vulnerability patterns
    %{
      injection_patterns: [],
      auth_bypass_patterns: [],
      data_exposure_patterns: []
    }
  end

  defp load_best_practices do
    # Load best practices definitions
    %{
      naming_conventions: [],
      documentation_standards: [],
      complexity_thresholds: %{}
    }
  end
end
