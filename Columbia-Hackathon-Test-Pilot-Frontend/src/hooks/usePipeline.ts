import { useState, useCallback, useRef } from "react";

export interface PipelineEvent {
  step: string;
  status: "running" | "done" | "error";
  items: string[];
  sandbox?: string;
  toolRows?: ToolRow[];
  testResults?: TestResultDetail[];
  passed?: number;
  total?: number;
}

export interface ToolRow {
  name: string;
  method: string;
  path: string;
  safety: string;
  execution: string;
  rateLimit: number;
}

export interface TestStepDetail {
  action: string;
  success: boolean;
  duration_ms: number;
  error: string;
}

export interface TestResultDetail {
  test_name: string;
  description: string;
  passed: boolean;
  duration_ms: number;
  summary: string;
  narrative: string;
  analysis: string;
  steps: TestStepDetail[];
}

const MAX_STEP = 11; // last valid index in PIPELINE_STEPS

// Maps backend step IDs to frontend step indices
const STEP_INDEX_MAP: Record<string, number> = {
  "clone": 1,
  "deploy-sandbox": 2,
  "extract": 3,
  "ingest": 4,
  "discover": 5,
  "schema": 6,
  "policy": 7,
  "generate": 8,
  "mcp-test": 9,
  "deploy": 10,
  "user-test": 11,
};

export interface StepData {
  items: string[];
  status: "pending" | "running" | "done" | "error";
  toolRows?: ToolRow[];
  testResults?: TestResultDetail[];
  passed?: number;
  total?: number;
  sandbox?: string;
}

export function usePipeline() {
  const [isRunning, setIsRunning] = useState(false);
  // activeStep = which step the pipeline is currently on (auto-tracked)
  const [activeStep, setActiveStep] = useState(0);
  // viewedStep = which step the user is looking at (click-navigated)
  const [viewedStep, setViewedStep] = useState(0);
  // whether the user has manually navigated away
  const userNavigatedRef = useRef(false);
  const [completedSteps, setCompletedSteps] = useState<Set<number>>(new Set());
  const [stepData, setStepData] = useState<Record<number, StepData>>({});
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const startPipeline = useCallback(async (urls: string[]) => {
    setIsRunning(true);
    setError(null);
    setCompletedSteps(new Set());
    setStepData({});
    userNavigatedRef.current = false;
    setActiveStep(1);
    setViewedStep(1);

    // Mark url-input as complete
    setCompletedSteps((prev) => {
      const next = new Set(prev);
      next.add(0);
      return next;
    });

    try {
      const apiBase = import.meta.env.VITE_API_URL ?? "";

      const startRes = await fetch(`${apiBase}/api/pipeline/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ urls }),
      });

      if (!startRes.ok) {
        throw new Error(`Failed to start pipeline: ${startRes.statusText}`);
      }

      const { run_id } = await startRes.json();

      const controller = new AbortController();
      abortRef.current = controller;

      const streamRes = await fetch(`${apiBase}/api/pipeline/stream/${run_id}`, {
        signal: controller.signal,
      });

      if (!streamRes.ok || !streamRes.body) {
        throw new Error("Failed to connect to pipeline stream");
      }

      const reader = streamRes.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const event: PipelineEvent = JSON.parse(line.slice(6));
            handleEvent(event);
          } catch {
            // skip malformed events
          }
        }
      }
    } catch (e: any) {
      if (e.name !== "AbortError") {
        setError(e.message || "Pipeline failed");
      }
    } finally {
      setIsRunning(false);
      abortRef.current = null;
    }
  }, []);

  const handleEvent = useCallback((event: PipelineEvent) => {
    if (event.step === "pipeline") {
      if (event.status === "error") {
        setError(event.items.join("; "));
      }
      return;
    }

    const stepIdx = STEP_INDEX_MAP[event.step];
    if (stepIdx === undefined) return;

    const data: StepData = {
      items: event.items,
      status: event.status === "done" ? "done" : event.status === "error" ? "error" : "running",
    };

    if (event.toolRows) data.toolRows = event.toolRows;
    if (event.testResults) data.testResults = event.testResults;
    if (event.passed !== undefined) data.passed = event.passed;
    if (event.total !== undefined) data.total = event.total;
    if (event.sandbox) data.sandbox = event.sandbox;

    setStepData((prev) => ({ ...prev, [stepIdx]: data }));

    if (event.status === "running") {
      const clamped = Math.min(stepIdx, MAX_STEP);
      setActiveStep(clamped);
      if (!userNavigatedRef.current) {
        setViewedStep(clamped);
      }
    } else if (event.status === "done") {
      setCompletedSteps((prev) => {
        const next = new Set(prev);
        next.add(stepIdx);
        return next;
      });
      const next = Math.min(stepIdx + 1, MAX_STEP);
      setActiveStep((prev) => Math.max(prev, next));
      if (!userNavigatedRef.current) {
        setViewedStep((prev) => Math.max(prev, next));
      }
    }
  }, []);

  const navigateToStep = useCallback((index: number) => {
    const clamped = Math.min(Math.max(index, 0), MAX_STEP);
    userNavigatedRef.current = true;
    setViewedStep(clamped);
  }, []);

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    setIsRunning(false);
  }, []);

  return {
    isRunning,
    activeStep,
    viewedStep,
    completedSteps,
    stepData,
    error,
    startPipeline,
    cancel,
    navigateToStep,
  };
}
