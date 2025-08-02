// Main JavaScript file for UFC Fight Analytics

$(document).ready(function() {
    // Initialize DataTables for all tables with class 'datatable'
    $('.datatable').DataTable({
        "pageLength": 25,
        "language": {
            "url": "//cdn.datatables.net/plug-ins/1.13.7/i18n/ja.json"
        },
        "responsive": true
    });

    // Add smooth scrolling for anchor links
    $('a[href*="#"]:not([href="#"])').on('click', function() {
        if (location.pathname.replace(/^\//, '') == this.pathname.replace(/^\//, '') && location.hostname == this.hostname) {
            var target = $(this.hash);
            target = target.length ? target : $('[name=' + this.hash.slice(1) + ']');
            if (target.length) {
                $('html, body').animate({
                    scrollTop: target.offset().top - 70
                }, 1000);
                return false;
            }
        }
    });

    // Tooltip initialization
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'))
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl)
    });

    // Format numbers with commas
    $('.number-format').each(function() {
        var num = parseInt($(this).text());
        $(this).text(num.toLocaleString());
    });
});

// Utility functions
function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('ja-JP', {
        year: 'numeric',
        month: 'long',
        day: 'numeric'
    });
}

function formatPercentage(value, decimals = 1) {
    return (value * 100).toFixed(decimals) + '%';
}

function getColorForPercentage(pct) {
    // Red to Yellow to Green gradient
    var percentColors = [
        { pct: 0.0, color: { r: 0xff, g: 0x00, b: 0 } },
        { pct: 0.5, color: { r: 0xff, g: 0xff, b: 0 } },
        { pct: 1.0, color: { r: 0x00, g: 0xff, b: 0 } }
    ];

    for (var i = 1; i < percentColors.length - 1; i++) {
        if (pct < percentColors[i].pct) {
            break;
        }
    }
    var lower = percentColors[i - 1];
    var upper = percentColors[i];
    var range = upper.pct - lower.pct;
    var rangePct = (pct - lower.pct) / range;
    var pctLower = 1 - rangePct;
    var pctUpper = rangePct;
    var color = {
        r: Math.floor(lower.color.r * pctLower + upper.color.r * pctUpper),
        g: Math.floor(lower.color.g * pctLower + upper.color.g * pctUpper),
        b: Math.floor(lower.color.b * pctLower + upper.color.b * pctUpper)
    };
    return 'rgb(' + [color.r, color.g, color.b].join(',') + ')';
}

// Create stat comparison bars
function createStatBar(containerId, value, maxValue, label) {
    const percentage = (value / maxValue) * 100;
    const container = document.getElementById(containerId);
    
    const html = `
        <div class="stat-bar">
            <div class="stat-bar-fill" style="width: ${percentage}%"></div>
            <div class="stat-bar-label">${label}: ${value}</div>
        </div>
    `;
    
    if (container) {
        container.innerHTML = html;
    }
}

// Format fight time
function formatFightTime(round, time) {
    if (!round || !time) return 'N/A';
    return `R${round} ${time}`;
}

// Get weight class icon
function getWeightClassIcon(weightClass) {
    const icons = {
        'Heavyweight': '🥊',
        'Light Heavyweight': '🥊',
        'Middleweight': '🥊',
        'Welterweight': '🥊',
        'Lightweight': '🥊',
        'Featherweight': '🥊',
        'Bantamweight': '🥊',
        'Flyweight': '🥊',
        'Women\'s Bantamweight': '👩‍🦰',
        'Women\'s Flyweight': '👩‍🦰',
        'Women\'s Strawweight': '👩‍🦰'
    };
    return icons[weightClass] || '🥊';
}