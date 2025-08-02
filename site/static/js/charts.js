// Chart.js configurations for UFC Fight Analytics

// Chart default options
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';
Chart.defaults.color = '#333';

// Finish Method Chart
if (document.getElementById('finishMethodChart') && typeof finishMethodData !== 'undefined') {
    const finishMethodCtx = document.getElementById('finishMethodChart').getContext('2d');
    new Chart(finishMethodCtx, {
        type: 'doughnut',
        data: {
            labels: finishMethodData.labels,
            datasets: [{
                data: finishMethodData.values,
                backgroundColor: [
                    '#c8102e', // UFC Red
                    '#ffb71b', // UFC Gold
                    '#333333', // Dark Gray
                    '#666666'  // Light Gray
                ],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 15,
                        font: {
                            size: 12
                        }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed;
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((value / total) * 100).toFixed(1);
                            return label + ': ' + value + ' (' + percentage + '%)';
                        }
                    }
                }
            }
        }
    });
}

// Weight Class Chart
if (document.getElementById('weightClassChart') && typeof weightClassData !== 'undefined') {
    const weightClassCtx = document.getElementById('weightClassChart').getContext('2d');
    new Chart(weightClassCtx, {
        type: 'bar',
        data: {
            labels: weightClassData.labels,
            datasets: [{
                label: '試合数',
                data: weightClassData.values,
                backgroundColor: '#c8102e',
                borderColor: '#a00d26',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return '試合数: ' + context.parsed.x;
                        }
                    }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    grid: {
                        display: true,
                        color: 'rgba(0, 0, 0, 0.05)'
                    }
                },
                y: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
}

// Odds Accuracy Over Time Chart
function createOddsAccuracyChart(containerId, data) {
    const ctx = document.getElementById(containerId).getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.labels,
            datasets: [{
                label: 'お気に入りの勝率',
                data: data.accuracy,
                borderColor: '#c8102e',
                backgroundColor: 'rgba(200, 16, 46, 0.1)',
                tension: 0.1,
                pointRadius: 4,
                pointHoverRadius: 6
            }, {
                label: '平均マージン',
                data: data.margin,
                borderColor: '#ffb71b',
                backgroundColor: 'rgba(255, 183, 27, 0.1)',
                tension: 0.1,
                pointRadius: 4,
                pointHoverRadius: 6,
                yAxisID: 'y1'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                legend: {
                    position: 'top',
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: '勝率 (%)'
                    },
                    min: 0,
                    max: 100
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'マージン (%)'
                    },
                    grid: {
                        drawOnChartArea: false,
                    },
                }
            }
        }
    });
}

// Fighter Performance Radar Chart
function createFighterRadarChart(containerId, data) {
    const ctx = document.getElementById(containerId).getContext('2d');
    new Chart(ctx, {
        type: 'radar',
        data: {
            labels: ['打撃精度', 'テイクダウン成功率', 'テイクダウン防御', '打撃防御', '終了率'],
            datasets: [{
                label: data.fighter1.name,
                data: data.fighter1.stats,
                borderColor: '#c8102e',
                backgroundColor: 'rgba(200, 16, 46, 0.2)',
                pointBackgroundColor: '#c8102e',
                pointBorderColor: '#fff',
                pointHoverBackgroundColor: '#fff',
                pointHoverBorderColor: '#c8102e'
            }, {
                label: data.fighter2.name,
                data: data.fighter2.stats,
                borderColor: '#ffb71b',
                backgroundColor: 'rgba(255, 183, 27, 0.2)',
                pointBackgroundColor: '#ffb71b',
                pointBorderColor: '#fff',
                pointHoverBackgroundColor: '#fff',
                pointHoverBorderColor: '#ffb71b'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                r: {
                    beginAtZero: true,
                    max: 100,
                    ticks: {
                        stepSize: 20
                    }
                }
            },
            plugins: {
                legend: {
                    position: 'top',
                }
            }
        }
    });
}

// Event Timeline Visualization
function createEventTimeline(containerId, events) {
    const container = document.getElementById(containerId);
    if (!container) return;

    let html = '<div class="event-timeline">';
    
    events.forEach((event, index) => {
        html += `
            <div class="event-timeline-item">
                <h6>${formatDate(event.date)}</h6>
                <p class="mb-1"><strong>${event.name}</strong></p>
                <small class="text-muted">${event.location} - ${event.fightCount}試合</small>
            </div>
        `;
    });
    
    html += '</div>';
    container.innerHTML = html;
}

// Strike Statistics Comparison
function createStrikeComparisonChart(containerId, data) {
    const ctx = document.getElementById(containerId).getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.rounds,
            datasets: [{
                label: data.fighter1.name,
                data: data.fighter1.strikes,
                backgroundColor: '#c8102e',
                borderColor: '#a00d26',
                borderWidth: 1
            }, {
                label: data.fighter2.name,
                data: data.fighter2.strikes,
                backgroundColor: '#ffb71b',
                borderColor: '#e5a319',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: '有効打数'
                    }
                }
            }
        }
    });
}