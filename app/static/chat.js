(function(){
  const dbFlag = document.getElementById('db_flag');
  const userId = document.getElementById('user_id');
  const sessionId = document.getElementById('session_id');
  const queryForm = document.getElementById('queryForm');
  const nlquery = document.getElementById('nlquery');
  const formatSelect = document.getElementById('format');
  const chatWindow = document.getElementById('chat_window');
  const resultViewer = document.getElementById('result_viewer');
  const resultMeta = document.getElementById('result_meta');
  const localHistory = document.getElementById('local_history');
  const clearHistoryBtn = document.getElementById('clear_history');

  const STORAGE_KEY = 'sql_insight_local_history_v1';

  function loadLocalHistory(){
    const raw = localStorage.getItem(STORAGE_KEY);
    try{
      return raw ? JSON.parse(raw) : [];
    }catch(e){
      console.warn('local history parse failed', e);
      return [];
    }
  }

  function saveLocalHistory(history){
    localStorage.setItem(STORAGE_KEY, JSON.stringify(history.slice(-10)));
  }

  function renderLocalHistory(){
    const history = loadLocalHistory();
    localHistory.innerHTML = '';
    if(history.length === 0){
      localHistory.innerText = 'No local conversation yet.';
      return;
    }
    history.slice(-3).reverse().forEach((entry, idx) =>{
      const el = document.createElement('div');
      el.className = 'local-entry';
      el.innerHTML = `<strong>Q:</strong> ${escapeHtml(entry.query)}<br/><strong>SQL:</strong> ${escapeHtml(entry.sql || '')}<br/><strong>Follow-ups:</strong> ${entry.follow_up_questions?.length ? escapeHtml(entry.follow_up_questions.join(', ')) : '[]'}<br/><small class="muted">${entry.time}</small>`;
      localHistory.appendChild(el);
    });
  }

  function escapeHtml(s){
    if(!s) return '';
    return s.replace(/&/g, '&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function appendMessage(role, text){
    const el = document.createElement('div');
    el.className = 'message ' + (role === 'user' ? 'user' : 'agent');
    el.innerText = text;
    chatWindow.appendChild(el);
    chatWindow.scrollTop = chatWindow.scrollHeight;
  }

  function clearResult(){
    resultViewer.innerHTML = '';
    resultMeta.innerHTML = '';
  }

  function showJSON(obj){
    resultViewer.innerHTML = `<pre>${escapeHtml(JSON.stringify(obj, null, 2))}</pre>`;
  }

  function renderCSV(csvText){
    // simple CSV -> table (naive, for dev/debug only)
    if(!csvText){
      resultViewer.innerText = 'No CSV payload.';
      return;
    }
    const rows = csvText.trim().split(/\r?\n/).map(r => r.split(','));
    const table = document.createElement('table');
    table.className = 'table-view';
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    rows[0].forEach(h => { const th = document.createElement('th'); th.innerText = h; headerRow.appendChild(th); });
    thead.appendChild(headerRow);
    table.appendChild(thead);
    const tbody = document.createElement('tbody');
    rows.slice(1, 200).forEach(r => {
      const tr = document.createElement('tr');
      r.forEach(cell => { const td = document.createElement('td'); td.innerText = cell; tr.appendChild(td); });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    resultViewer.innerHTML = '';
    resultViewer.appendChild(table);

    const download = document.createElement('a');
    download.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csvText);
    download.download = 'query_results.csv';
    download.innerText = 'Download CSV';
    download.style.display = 'inline-block';
    download.style.marginTop = '8px';
    resultViewer.appendChild(download);
  }

  queryForm.addEventListener('submit', async (ev) =>{
    ev.preventDefault();
    clearResult();
    const q = nlquery.value.trim();
    if(!q) return;

    appendMessage('user', q);

    const payload = {
      query: q,
      db_flag: dbFlag.value || 'avamed_db',
      output_format: formatSelect.value || 'json',
      user_id: userId.value || undefined,
      session_id: sessionId.value || undefined,
    };

    appendMessage('agent', 'Thinking...');

    try{
      const resp = await fetch('/query', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
      });
      const json = await resp.json();
      // Remove last 'Thinking...' message
      const msgs = chatWindow.querySelectorAll('.message.agent');
      if(msgs && msgs.length) msgs[msgs.length - 1].remove();

      if(!resp.ok){
        appendMessage('agent', `Error: ${json.detail || resp.statusText}`);
        return;
      }

      // Show SQL if present
      const sql = json.sql || '';
      let agentText = sql ? `SQL: ${sql}` : 'No SQL returned';
      if(json.follow_up_questions && json.follow_up_questions.length){
        agentText += `\nSuggested next steps: ${json.follow_up_questions.join(' | ')}`;
      }

      appendMessage('agent', agentText);

      // Show results
      const data = json.data || {};
      resultMeta.innerText = `Rows: ${json.metadata?.total_rows ?? data.row_count ?? 'unknown'} | Execution time(ms): ${json.metadata?.execution_time_ms ?? 'n/a'}`;

      // Prefer CSV if requested or available
      if(payload.output_format === 'csv' && data.csv){
        renderCSV(data.csv);
      } else if(data.csv){
        // if CSV exists but output_format != csv, still render a preview
        renderCSV(data.csv);
      } else if(data.raw_json){
        try{
          showJSON(JSON.parse(data.raw_json));
        }catch(e){
          showJSON(data.raw_json);
        }
      } else {
        showJSON(data || json);
      }

      // store local history snapshot
      const localHist = loadLocalHistory();
      localHist.push({ query: payload.query, sql: sql, follow_up_questions: json.follow_up_questions || [], time: new Date().toISOString() });
      saveLocalHistory(localHist);
      renderLocalHistory();

    }catch(err){
      console.error('request failed', err);
      appendMessage('agent', `Request failed: ${err.message}`);
    }
  });

  clearHistoryBtn.addEventListener('click', ()=>{
    localStorage.removeItem(STORAGE_KEY);
    renderLocalHistory();
  });

  // initial render of local history
  renderLocalHistory();
})();
