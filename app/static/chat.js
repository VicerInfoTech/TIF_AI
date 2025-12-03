(function () {
  const dbFlag = document.getElementById('db_flag');
  const userId = document.getElementById('user_id');
  const sessionId = document.getElementById('session_id');
  const queryForm = document.getElementById('queryForm');
  const nlquery = document.getElementById('nlquery');
  const formatSelect = document.getElementById('format');
  const chatWindow = document.getElementById('chat_window');
  const localHistory = document.getElementById('local_history');
  const clearHistoryBtn = document.getElementById('clear_history');

  const STORAGE_KEY = 'sql_insight_local_history_v2';

  function loadLocalHistory() {
    const raw = localStorage.getItem(STORAGE_KEY);
    try {
      return raw ? JSON.parse(raw) : [];
    } catch (e) {
      console.warn('local history parse failed', e);
      return [];
    }
  }

  function saveLocalHistory(history) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(history.slice(-10)));
  }

  function renderLocalHistory() {
    const history = loadLocalHistory();
    localHistory.innerHTML = '';
    if (history.length === 0) {
      localHistory.innerHTML = '<div style="padding:0.75rem; color:var(--text-muted); font-size:0.75rem;">No history yet.</div>';
      return;
    }
    history.slice().reverse().forEach((entry) => {
      const el = document.createElement('div');
      el.className = 'local-entry';
      el.innerHTML = `<strong>Q:</strong> ${escapeHtml(entry.query)}<br/><small style="color:var(--text-muted)">${new Date(entry.time).toLocaleTimeString()}</small>`;
      el.onclick = () => {
        nlquery.value = entry.query;
        nlquery.focus();
      };
      localHistory.appendChild(el);
    });
  }

  function escapeHtml(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  }

  function createMessageElement(role, contentHtml) {
    const el = document.createElement('div');
    el.className = 'message ' + role;
    const contentDiv = document.createElement('div');
    contentDiv.className = 'message-content';
    contentDiv.innerHTML = contentHtml;
    el.appendChild(contentDiv);
    return el;
  }

  function appendMessage(role, text) {
    const el = createMessageElement(role, escapeHtml(text).replace(/\n/g, '<br/>'));
    chatWindow.appendChild(el);
    scrollToBottom();
    return el;
  }

  function appendHtmlMessage(role, html) {
    const el = createMessageElement(role, html);
    chatWindow.appendChild(el);
    scrollToBottom();
    return el;
  }

  function scrollToBottom() {
    chatWindow.scrollTop = chatWindow.scrollHeight;
  }

  function createDownloadButton(content, filename, mimeType, buttonText) {
    let blobContent = content;
    if (filename.endsWith('.csv')) {
      const BOM = '\uFEFF';
      blobContent = BOM + content;
    }

    const blob = new Blob([blobContent], { type: mimeType + ';charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const linkId = 'download_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);

    return `<div style="margin-top:1rem;"><a id="${linkId}" href="${url}" download="${filename}" style="display:inline-block; padding:0.5rem 1rem; background:var(--primary-color); color:white; text-decoration:none; border-radius:0.375rem; font-size:0.875rem; font-weight:500; transition:background 0.2s; cursor:pointer;" onmouseover="this.style.background='var(--primary-hover)'" onmouseout="this.style.background='var(--primary-color)'">${buttonText}</a></div>`;
  }

  function renderTable(csvText) {
    if (!csvText) return '<div style="color:var(--text-muted)">No data returned.</div>';

    const rows = csvText.trim().split(/\r?\n/).map(r => {
      const fields = [];
      let current = '';
      let inQuotes = false;
      for (let i = 0; i < r.length; i++) {
        const char = r[i];
        if (char === '"') {
          inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
          fields.push(current);
          current = '';
        } else {
          current += char;
        }
      }
      fields.push(current);
      return fields.map(f => f.replace(/^"|"$/g, '').trim());
    });

    if (rows.length === 0) return '';

    let html = '<div class="result-container"><div class="table-wrapper"><table class="table-view"><thead><tr>';
    rows[0].forEach(h => {
      html += `<th>${escapeHtml(h)}</th>`;
    });
    html += '</tr></thead><tbody>';

    const maxRows = Math.min(rows.length - 1, 100);
    for (let i = 1; i <= maxRows; i++) {
      html += '<tr>';
      rows[i].forEach(cell => {
        html += `<td>${escapeHtml(cell)}</td>`;
      });
      html += '</tr>';
    }

    html += '</tbody></table></div></div>';

    if (rows.length > 101) {
      html += `<div style="padding:0.5rem; font-size:0.75rem; color:var(--text-muted); text-align:center;">Showing first 100 rows of ${rows.length - 1} total</div>`;
    }

    return html;
  }

  function renderJSON(obj) {
    let jsonStr = JSON.stringify(obj, null, 2);
    return `<pre><code>${escapeHtml(jsonStr)}</code></pre>`;
  }

  function csvToJson(csvText) {
    if (!csvText) return [];

    const lines = csvText.trim().split(/\r?\n/);
    if (lines.length < 2) return [];

    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    const data = [];

    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split(',').map(v => v.trim().replace(/^"|"$/g, ''));
      const obj = {};
      headers.forEach((header, index) => {
        const value = values[index] || '';
        // Try to parse as number
        const numValue = parseFloat(value);
        obj[header] = isNaN(numValue) ? value : numValue;
      });
      data.push(obj);
    }

    return data;
  }

  function isChartable(data) {
    if (!data || data.length === 0) return false;

    const keys = Object.keys(data[0]);
    if (keys.length < 2) return false;

    // Check if at least one column has numeric values
    const hasNumeric = keys.some(key => {
      return data.every(row => typeof row[key] === 'number');
    });

    return hasNumeric;
  }

  function renderChart(data, chartType = 'bar') {
    const canvasId = 'chart_' + Date.now();
    const keys = Object.keys(data[0]);
    console.log("keys------------------------------------------------------------", keys);

    // Find text column (for labels) and numeric column (for values)
    let labelKey = keys[0];
    let dataKey = keys.find(k => typeof data[0][k] === 'number') || keys[1];

    // If first column is numeric, swap
    if (typeof data[0][labelKey] === 'number' && keys.length > 1) {
      labelKey = keys.find(k => typeof data[0][k] !== 'number') || keys[0];
    }

    const labels = data.map(row => String(row[labelKey]));
    const values = data.map(row => parseFloat(row[dataKey]) || 0);

    // Create toggle buttons for different chart types
    const toggleButtonsId = 'toggle_' + Date.now();

    const html = `
      <div style="margin:1rem 0; padding:1rem; background:rgba(0,0,0,0.02); border-radius:0.5rem;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem;">
          <strong style="color:var(--text-primary); font-size:0.875rem;">📊 Data Visualization</strong>
          <div id="${toggleButtonsId}" style="display:flex; gap:0.5rem;">
            <button class="chart-toggle" data-canvas="${canvasId}" data-type="bar" style="padding:0.25rem 0.75rem; border:1px solid var(--primary-color); background:var(--primary-color); color:white; border-radius:0.25rem; cursor:pointer; font-size:0.75rem; transition:all 0.2s;">Bar</button>
            <button class="chart-toggle" data-canvas="${canvasId}" data-type="line" style="padding:0.25rem 0.75rem; border:1px solid var(--primary-color); background:white; color:var(--primary-color); border-radius:0.25rem; cursor:pointer; font-size:0.75rem; transition:all 0.2s;">Line</button>
            <button class="chart-toggle" data-canvas="${canvasId}" data-type="pie" style="padding:0.25rem 0.75rem; border:1px solid var(--primary-color); background:white; color:var(--primary-color); border-radius:0.25rem; cursor:pointer; font-size:0.75rem; transition:all 0.2s;">Pie</button>
            <button class="chart-toggle" data-canvas="${canvasId}" data-type="doughnut" style="padding:0.25rem 0.75rem; border:1px solid var(--primary-color); background:white; color:var(--primary-color); border-radius:0.25rem; cursor:pointer; font-size:0.75rem; transition:all 0.2s;">Doughnut</button>
          </div>
        </div>
        <div style="max-width:700px; max-height:400px; margin:0 auto;">
          <canvas id="${canvasId}"></canvas>
        </div>
      </div>
    `;

    // Render chart after DOM is ready
    setTimeout(() => {
      const ctx = document.getElementById(canvasId);
      if (!ctx) return;

      const chartConfig = {
        type: chartType,
        data: {
          labels: labels,
          datasets: [{
            label: dataKey,
            data: values,
            backgroundColor: chartType === 'bar' || chartType === 'line'
              ? 'rgba(59, 130, 246, 0.6)'
              : values.map((_, i) => `hsla(${(i * 360) / values.length}, 70%, 60%, 0.8)`),
            borderColor: chartType === 'line'
              ? 'rgba(59, 130, 246, 1)'
              : values.map((_, i) => `hsla(${(i * 360) / values.length}, 70%, 50%, 1)`),
            borderWidth: 2,
            tension: 0.4
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          plugins: {
            legend: {
              display: chartType === 'pie' || chartType === 'doughnut',
              position: 'bottom'
            },
            title: {
              display: true,
              text: `${dataKey} by ${labelKey}`,
              font: { size: 14, weight: '600' }
            },
            tooltip: {
              backgroundColor: 'rgba(0, 0, 0, 0.8)',
              padding: 12,
              titleFont: { size: 13 },
              bodyFont: { size: 12 }
            }
          },
          scales: chartType === 'bar' || chartType === 'line' ? {
            y: {
              beginAtZero: true,
              grid: { color: 'rgba(0,0,0,0.05)' }
            },
            x: {
              grid: { display: false }
            }
          } : undefined
        }
      };

      const myChart = new Chart(ctx, chartConfig);

      // Add toggle functionality
      document.querySelectorAll(`#${toggleButtonsId} .chart-toggle`).forEach(btn => {
        btn.addEventListener('click', function () {
          const newType = this.dataset.type;

          // Update button styles
          document.querySelectorAll(`#${toggleButtonsId} .chart-toggle`).forEach(b => {
            b.style.background = 'white';
            b.style.color = 'var(--primary-color)';
          });
          this.style.background = 'var(--primary-color)';
          this.style.color = 'white';

          // Update chart
          myChart.config.type = newType;
          myChart.options.plugins.legend.display = newType === 'pie' || newType === 'doughnut';
          myChart.options.scales = (newType === 'bar' || newType === 'line') ? {
            y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
            x: { grid: { display: false } }
          } : undefined;

          // Update colors
          if (newType === 'pie' || newType === 'doughnut') {
            myChart.data.datasets[0].backgroundColor = values.map((_, i) =>
              `hsla(${(i * 360) / values.length}, 70%, 60%, 0.8)`
            );
            myChart.data.datasets[0].borderColor = values.map((_, i) =>
              `hsla(${(i * 360) / values.length}, 70%, 50%, 1)`
            );
          } else {
            myChart.data.datasets[0].backgroundColor = 'rgba(59, 130, 246, 0.6)';
            myChart.data.datasets[0].borderColor = newType === 'line' ? 'rgba(59, 130, 246, 1)' : 'rgba(59, 130, 246, 0.6)';
          }

          myChart.update();
        });
      });
    }, 100);

    return html;
  }


  queryForm.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const q = nlquery.value.trim();
    if (!q) return;

    appendMessage('user', q);
    nlquery.value = '';

    const selectedFormat = formatSelect.value || 'json';
    const outputFormat = selectedFormat === 'table' ? 'csv' : selectedFormat;

    const payload = {
      query: q,
      db_flag: dbFlag.value || 'your_db_flag',
      output_format: outputFormat,
      user_id: userId.value || undefined,
      session_id: sessionId.value || undefined,
    };

    const loadingMsg = appendMessage('agent', 'Thinking...');

    try {
      const resp = await fetch('/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      loadingMsg.remove();
      const json = await resp.json();

      if (!resp.ok) {
        const errorMsg = json.error || json.detail || resp.statusText;
        const errorCode = json.error_code ? ` (${json.error_code})` : '';
        appendMessage('agent', `❌ Error${errorCode}: ${errorMsg}`);
        return;
      }

      const sql = json.sql || '';
      let agentText = '';

      if (json.natural_summary) {
        agentText += `<div style="margin-bottom:1rem; padding:0.75rem; background:rgba(59, 130, 246, 0.1); border-left:3px solid var(--primary-color); border-radius:0.375rem;">${escapeHtml(json.natural_summary)}</div>`;
      }

      // SQL display commented out
      // if (sql) {
      //   agentText += `<div style="margin-bottom:0.5rem; font-size:0.75rem; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.05em;">Generated SQL</div><pre><code>${escapeHtml(sql)}</code></pre>`;
      // } else {
      //   agentText += '<div style="color:var(--text-muted);">No SQL generated</div>';
      // }

      if (json.follow_up_questions && json.follow_up_questions.length) {
        agentText += `<div style="margin-top:1rem;"><strong style="font-size:0.75rem; text-transform:uppercase; color:var(--text-muted);">💡 Suggested Questions</strong><ul style="margin-top:0.5rem; list-style:none; padding:0;">`;
        json.follow_up_questions.forEach(q => {
          const escapedQ = escapeHtml(q).replace(/'/g, "\\'");
          agentText += `<li style="cursor:pointer; transition:all 0.2s; color:var(--primary-color); padding:0.5rem; margin:0.25rem 0; border-radius:0.375rem; background:rgba(59,130,246,0.05);" onmouseover="this.style.background='rgba(59,130,246,0.15)'; this.style.transform='translateX(4px)';" onmouseout="this.style.background='rgba(59,130,246,0.05)'; this.style.transform='translateX(0)';" onclick="(function(){document.getElementById('nlquery').value='${escapedQ}';document.getElementById('queryForm').dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}));})()">${escapeHtml(q)}</li>`;
        });
        agentText += `</ul></div>`;
      }

      appendHtmlMessage('agent', agentText);

      const result = json.result || {};
      let dataHtml = '';

      const rows = result.row_count ?? 'N/A';
      const time = json.metadata?.execution_time_ms ? Math.round(json.metadata.execution_time_ms) : 'N/A';
      const metaHtml = `<div class="message-meta">📊 Rows: ${rows} | ⏱️ Time: ${time}ms</div>`;

      if (result.content) {
        if (result.filetype === 'csv') {
          const tableHtml = renderTable(result.content);

          // Try to render chart if data is numeric and chartable
          try {
            const jsonData = csvToJson(result.content);
            if (jsonData.length > 0 && jsonData.length <= 50 && isChartable(jsonData)) {
              // Render chart above table for chartable data
              dataHtml = renderChart(jsonData, 'bar') + tableHtml;
            } else {
              // Just table if not chartable or too many rows
              dataHtml = tableHtml;
            }
          } catch (e) {
            console.warn('Chart rendering skipped:', e);
            dataHtml = tableHtml;
          }

          dataHtml += createDownloadButton(result.content, 'query_results.csv', 'text/csv', '📥 Download CSV');
        } else {
          try {
            const parsedJson = JSON.parse(result.content);
            dataHtml = renderJSON(parsedJson);
            dataHtml += createDownloadButton(JSON.stringify(parsedJson, null, 2), 'query_results.json', 'application/json', '📥 Download JSON');
          } catch (e) {
            dataHtml = `<pre><code>${escapeHtml(result.content)}</code></pre>`;
            dataHtml += createDownloadButton(result.content, 'query_results.json', 'application/json', '📥 Download JSON');
          }
        }
      } else {
        dataHtml = '<div style="color:var(--text-muted)">No data returned.</div>';
      }

      if (dataHtml) {
        appendHtmlMessage('agent', dataHtml + metaHtml);
      }

      const localHist = loadLocalHistory();
      localHist.push({
        query: q,
        sql: sql,
        time: new Date().toISOString()
      });
      saveLocalHistory(localHist);
      renderLocalHistory();

    } catch (err) {
      loadingMsg.remove();
      console.error('request failed', err);
      appendMessage('agent', `❌ Request failed: ${err.message}`);
    }
  });

  clearHistoryBtn.addEventListener('click', () => {
    if (confirm('Clear all local history?')) {
      localStorage.removeItem(STORAGE_KEY);
      renderLocalHistory();
    }
  });

  renderLocalHistory();
})();
