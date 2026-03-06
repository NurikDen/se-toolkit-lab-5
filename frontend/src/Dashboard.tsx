import React, { useEffect, useState, useMemo } from 'react';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    LineElement,
    PointElement,
    Title,
    Tooltip,
    Legend,
    type ChartData,
    type ChartOptions,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';

// ---------------------------------------------------------------------------
// Chart.js Registration (required for react-chartjs-2 to work)
// ---------------------------------------------------------------------------

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    LineElement,
    PointElement,
    Title,
    Tooltip,
    Legend
);

// ---------------------------------------------------------------------------
// TypeScript Types for API Responses
// ---------------------------------------------------------------------------

interface ScoreBucket {
    bucket: string;
    count: number;
}

interface PassRateItem {
    task: string;
    avg_score: number;
    attempts: number;
}

interface TimelineItem {
    date: string;
    submissions: number;
}

interface LabOption {
    id: string;
    name: string;
}

// ---------------------------------------------------------------------------
// API Client Helpers
// ---------------------------------------------------------------------------

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

function getAuthHeaders(): HeadersInit {
    const apiKey = localStorage.getItem('api_key');
    return {
        'Content-Type': 'application/json',
        ...(apiKey ? { Authorization: `Bearer ${apiKey}` } : {}),
    };
}

async function fetchScores(labId: string): Promise<ScoreBucket[]> {
    const response = await fetch(`${API_BASE_URL}/analytics/scores?lab=${labId}`, {
        headers: getAuthHeaders(),
    });
    if (!response.ok) {
        throw new Error(`Failed to fetch scores: ${response.status}`);
    }
    return response.json() as Promise<ScoreBucket[]>;
}

async function fetchTimeline(labId: string): Promise<TimelineItem[]> {
    const response = await fetch(`${API_BASE_URL}/analytics/timeline?lab=${labId}`, {
        headers: getAuthHeaders(),
    });
    if (!response.ok) {
        throw new Error(`Failed to fetch timeline: ${response.status}`);
    }
    return response.json() as Promise<TimelineItem[]>;
}

async function fetchPassRates(labId: string): Promise<PassRateItem[]> {
    const response = await fetch(`${API_BASE_URL}/analytics/pass-rates?lab=${labId}`, {
        headers: getAuthHeaders(),
    });
    if (!response.ok) {
        throw new Error(`Failed to fetch pass rates: ${response.status}`);
    }
    return response.json() as Promise<PassRateItem[]>;
}

// ---------------------------------------------------------------------------
// Chart Data Transformers
// ---------------------------------------------------------------------------

function transformScoresToChartData(scores: ScoreBucket[]): ChartData<'bar'> {
    const labels = scores.map((s) => s.bucket);
    const data = scores.map((s) => s.count);

    return {
        labels,
        datasets: [
            {
                label: 'Number of Students',
                data,
                backgroundColor: 'rgba(54, 162, 235, 0.6)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1,
            },
        ],
    };
}

function transformTimelineToChartData(timeline: TimelineItem[]): ChartData<'line'> {
    const labels = timeline.map((t) => t.date);
    const data = timeline.map((t) => t.submissions);

    return {
        labels,
        datasets: [
            {
                label: 'Submissions per Day',
                data,
                borderColor: 'rgba(75, 192, 192, 1)',
                backgroundColor: 'rgba(75, 192, 192, 0.2)',
                tension: 0.3,
                fill: true,
            },
        ],
    };
}

// ---------------------------------------------------------------------------
// Main Dashboard Component
// ---------------------------------------------------------------------------

interface DashboardState {
    scores: ScoreBucket[] | null;
    timeline: TimelineItem[] | null;
    passRates: PassRateItem[] | null;
    loading: boolean;
    error: string | null;
}

const AVAILABLE_LABS: LabOption[] = [
    { id: 'lab-01', name: 'Lab 01' },
    { id: 'lab-02', name: 'Lab 02' },
    { id: 'lab-03', name: 'Lab 03' },
    { id: 'lab-04', name: 'Lab 04' },
];

const DEFAULT_LAB = 'lab-04';

export const Dashboard: React.FC = () => {
    const [selectedLab, setSelectedLab] = useState<string>(DEFAULT_LAB);
    const [state, setState] = useState<DashboardState>({
        scores: null,
        timeline: null,
        passRates: null,
        loading: true,
        error: null,
    });

    // Fetch all analytics data when lab changes
    useEffect(() => {
        const loadData = async (): Promise<void> => {
            setState((prev) => ({ ...prev, loading: true, error: null }));

            try {
                const [scores, timeline, passRates] = await Promise.all([
                    fetchScores(selectedLab),
                    fetchTimeline(selectedLab),
                    fetchPassRates(selectedLab),
                ]);

                setState({
                    scores,
                    timeline,
                    passRates,
                    loading: false,
                    error: null,
                });
            } catch (err) {
                setState((prev) => ({
                    ...prev,
                    loading: false,
                    error: err instanceof Error ? err.message : 'Unknown error occurred',
                }));
            }
        };

        void loadData();
    }, [selectedLab]);

    // Memoize chart data to prevent unnecessary re-renders
    const scoresChartData = useMemo<ChartData<'bar'> | null>(() => {
        if (!state.scores) return null;
        return transformScoresToChartData(state.scores);
    }, [state.scores]);

    const timelineChartData = useMemo<ChartData<'line'> | null>(() => {
        if (!state.timeline) return null;
        return transformTimelineToChartData(state.timeline);
    }, [state.timeline]);

    // Chart options (type-safe)
    const barChartOptions: ChartOptions<'bar'> = {
        responsive: true,
        plugins: {
            legend: {
                position: 'top' as const,
            },
            title: {
                display: true,
                text: 'Score Distribution',
            },
        },
        scales: {
            y: {
                beginAtZero: true,
                ticks: {
                    stepSize: 1,
                },
            },
        },
    };

    const lineChartOptions: ChartOptions<'line'> = {
        responsive: true,
        plugins: {
            legend: {
                position: 'top' as const,
            },
            title: {
                display: true,
                text: 'Submissions Timeline',
            },
        },
        scales: {
            y: {
                beginAtZero: true,
                ticks: {
                    stepSize: 1,
                },
            },
        },
    };

    // -----------------------------------------------------------------------
    // Render Functions
    // -----------------------------------------------------------------------

    const renderLabSelector = (): JSX.Element => (
        <div className="mb-6">
            <label htmlFor="lab-select" className="block text-sm font-medium text-gray-700 mb-2">
                Select Lab
            </label>
            <select
                id="lab-select"
                value={selectedLab}
                onChange={(e) => setSelectedLab(e.target.value)}
                className="block w-full md:w-64 px-4 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
            >
                {AVAILABLE_LABS.map((lab) => (
                    <option key={lab.id} value={lab.id}>
                        {lab.name}
                    </option>
                ))}
            </select>
        </div>
    );

    const renderLoadingState = (): JSX.Element => (
        <div className="flex items-center justify-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            <span className="ml-4 text-gray-600">Loading analytics data...</span>
        </div>
    );

    const renderErrorState = (): JSX.Element => (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-red-800 font-medium">Error loading data</p>
            <p className="text-red-600 text-sm mt-1">{state.error}</p>
            <button
                onClick={() => setState((prev) => ({ ...prev, loading: true, error: null }))}
                className="mt-3 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 transition-colors"
            >
                Retry
            </button>
        </div>
    );

    const renderScoresChart = (): JSX.Element | null => {
        if (!scoresChartData) return null;

        return (
            <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Score Distribution</h3>
                <div className="h-64">
                    <Bar data={scoresChartData} options={barChartOptions} />
                </div>
            </div>
        );
    };

    const renderTimelineChart = (): JSX.Element | null => {
        if (!timelineChartData) return null;

        return (
            <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Submissions Timeline</h3>
                <div className="h-64">
                    <Line data={timelineChartData} options={lineChartOptions} />
                </div>
            </div>
        );
    };

    const renderPassRatesTable = (): JSX.Element | null => {
        if (!state.passRates) return null;

        return (
            <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Pass Rates by Task</h3>
                <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                            <tr>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                    Task
                                </th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                    Average Score
                                </th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                    Attempts
                                </th>
                            </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                            {state.passRates.map((item, index) => (
                                <tr key={index}>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                                        {item.task}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {item.avg_score.toFixed(1)}%
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {item.attempts}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        );
    };

    // -----------------------------------------------------------------------
    // Main Render
    // -----------------------------------------------------------------------

    return (
        <div className="min-h-screen bg-gray-100 p-6">
            <div className="max-w-7xl mx-auto">
                <h1 className="text-3xl font-bold text-gray-900 mb-6">Analytics Dashboard</h1>

                {renderLabSelector()}

                {state.loading ? (
                    renderLoadingState()
                ) : state.error ? (
                    renderErrorState()
                ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {renderScoresChart()}
                        {renderTimelineChart()}
                        <div className="md:col-span-2">{renderPassRatesTable()}</div>
                    </div>
                )}
            </div>
        </div>
    );
};

export default Dashboard;