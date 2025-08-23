defmodule Presto.Debugging.ExecutionTracer do
  @moduledoc """
  Execution tracer for debugging rule execution flow.

  Provides detailed tracing capabilities for rule execution including:
  - Rule firing events
  - Condition evaluation traces
  - Action execution monitoring
  - Performance metrics collection
  """

  use GenServer
  require Logger

  @type trace_event :: %{
          id: String.t(),
          timestamp: DateTime.t(),
          event_type: atom(),
          rule_id: atom(),
          details: map(),
          duration_ms: number()
        }

  @type tracer_state :: %{
          active: boolean(),
          traces: [trace_event()],
          max_traces: pos_integer(),
          trace_filter: [atom()],
          subscribers: [pid()]
        }

  ## Client API

  @doc """
  Starts the execution tracer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts tracing execution events.
  """
  @spec start_tracing(keyword()) :: :ok
  def start_tracing(opts \\ []) do
    GenServer.call(__MODULE__, {:start_tracing, opts})
  end

  @doc """
  Stops tracing execution events.
  """
  @spec stop_tracing() :: :ok
  def stop_tracing do
    GenServer.call(__MODULE__, :stop_tracing)
  end

  @doc """
  Records a trace event.
  """
  @spec trace_event(atom(), atom(), map()) :: :ok
  def trace_event(event_type, rule_id, details \\ %{}) do
    GenServer.cast(__MODULE__, {:trace_event, event_type, rule_id, details})
  end

  @doc """
  Gets current trace events.
  """
  @spec get_traces(keyword()) :: [trace_event()]
  def get_traces(opts \\ []) do
    GenServer.call(__MODULE__, {:get_traces, opts})
  end

  @doc """
  Clears all trace events.
  """
  @spec clear_traces() :: :ok
  def clear_traces do
    GenServer.call(__MODULE__, :clear_traces)
  end

  @doc """
  Subscribes to trace events.
  """
  @spec subscribe(pid()) :: :ok
  def subscribe(subscriber_pid) do
    GenServer.call(__MODULE__, {:subscribe, subscriber_pid})
  end

  @doc """
  Unsubscribes from trace events.
  """
  @spec unsubscribe(pid()) :: :ok
  def unsubscribe(subscriber_pid) do
    GenServer.call(__MODULE__, {:unsubscribe, subscriber_pid})
  end

  ## Server implementation

  @impl GenServer
  def init(opts) do
    state = %{
      active: Keyword.get(opts, :active, false),
      traces: [],
      max_traces: Keyword.get(opts, :max_traces, 1000),
      trace_filter: Keyword.get(opts, :trace_filter, [:all]),
      subscribers: []
    }

    Logger.info("Starting Execution Tracer")
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:start_tracing, opts}, _from, state) do
    new_state = %{
      state
      | active: true,
        trace_filter: Keyword.get(opts, :filter, state.trace_filter),
        max_traces: Keyword.get(opts, :max_traces, state.max_traces)
    }

    Logger.info("Started execution tracing", filter: new_state.trace_filter)
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_call(:stop_tracing, _from, state) do
    new_state = %{state | active: false}
    Logger.info("Stopped execution tracing")
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_call({:get_traces, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, length(state.traces))
    filter = Keyword.get(opts, :filter, :all)

    filtered_traces =
      state.traces
      |> filter_traces(filter)
      |> Enum.take(limit)

    {:reply, filtered_traces, state}
  end

  @impl GenServer
  def handle_call(:clear_traces, _from, state) do
    new_state = %{state | traces: []}
    Logger.info("Cleared all trace events")
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_call({:subscribe, subscriber_pid}, _from, state) do
    if subscriber_pid in state.subscribers do
      {:reply, :ok, state}
    else
      new_subscribers = [subscriber_pid | state.subscribers]
      Process.monitor(subscriber_pid)
      {:reply, :ok, %{state | subscribers: new_subscribers}}
    end
  end

  @impl GenServer
  def handle_call({:unsubscribe, subscriber_pid}, _from, state) do
    new_subscribers = List.delete(state.subscribers, subscriber_pid)
    {:reply, :ok, %{state | subscribers: new_subscribers}}
  end

  @impl GenServer
  def handle_cast({:trace_event, event_type, rule_id, details}, state) do
    if state.active and should_trace_event?(event_type, state.trace_filter) do
      trace_event = create_trace_event(event_type, rule_id, details)
      new_traces = add_trace_event(trace_event, state.traces, state.max_traces)

      # Notify subscribers
      notify_subscribers(trace_event, state.subscribers)

      {:noreply, %{state | traces: new_traces}}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Remove dead subscriber
    new_subscribers = List.delete(state.subscribers, pid)
    {:noreply, %{state | subscribers: new_subscribers}}
  end

  ## Private functions

  defp should_trace_event?(_event_type, [:all]), do: true
  defp should_trace_event?(event_type, filter), do: event_type in filter

  defp create_trace_event(event_type, rule_id, details) do
    %{
      id: generate_trace_id(),
      timestamp: DateTime.utc_now(),
      event_type: event_type,
      rule_id: rule_id,
      details: details,
      duration_ms: Map.get(details, :duration_ms, 0)
    }
  end

  defp add_trace_event(trace_event, traces, max_traces) do
    [trace_event | Enum.take(traces, max_traces - 1)]
  end

  defp notify_subscribers(trace_event, subscribers) do
    Enum.each(subscribers, fn subscriber ->
      send(subscriber, {:trace_event, trace_event})
    end)
  end

  defp filter_traces(traces, :all), do: traces

  defp filter_traces(traces, event_types) when is_list(event_types) do
    Enum.filter(traces, fn trace -> trace.event_type in event_types end)
  end

  defp filter_traces(traces, event_type) do
    Enum.filter(traces, fn trace -> trace.event_type == event_type end)
  end

  defp generate_trace_id do
    "trace_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  end
end
