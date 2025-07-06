from flask import Flask, request, redirect, url_for, render_template_string, jsonify
import os
import markdown2
import re
import glob

app = Flask(__name__)
DATA_DIR = '/var/lib/data'
os.makedirs(DATA_DIR, exist_ok=True)

def get_markdown_file(page):
    safe_page = page.replace('/', '_')
    return os.path.join(DATA_DIR, f"{safe_page}.md")

def render_markdown(content):
    html = markdown2.markdown(content, extras=["fenced-code-blocks"])
    html = re.sub(
        r'<pre><code class="language-mermaid">(.*?)</code></pre>',
        r'<div class="mermaid">\1</div>',
        html, flags=re.DOTALL
    )
    html = re.sub(r'\[\[([^\]]+)\]\]', r'<a href="/page/\1">\1</a>', html)
    return html

def get_all_pages():
    """Get all page names from the data directory"""
    pages = []
    for file_path in glob.glob(os.path.join(DATA_DIR, "*.md")):
        page_name = os.path.basename(file_path)[:-3]  # Remove .md extension
        page_name = page_name.replace('_', '/')  # Convert back from safe filename
        pages.append(page_name)
    return pages

def find_backlinks(target_page):
    """Find all pages that link to the target page"""
    backlinks = []
    all_pages = get_all_pages()
    
    for page in all_pages:
        if page == target_page:
            continue
            
        file_path = get_markdown_file(page)
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Look for [[target_page]] links
            if f'[[{target_page}]]' in content:
                backlinks.append(page)
    
    return backlinks

@app.route('/')
def index():
    return redirect(url_for('view_page', page='Home'))

@app.route('/page/<page>', methods=['GET', 'POST'])
def view_page(page):
    file_path = get_markdown_file(page)

    if request.method == 'POST':
        with open(file_path, 'w') as f:
            f.write(request.form['content'])
        return redirect(url_for('view_page', page=page))

    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            f.write(f"# {page}\n")

    with open(file_path, 'r') as f:
        content = f.read()

    rendered = render_markdown(content)
    backlinks = find_backlinks(page)

    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
    <title>{{ page }}</title>
    <meta charset="utf-8">
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>mermaid.initialize({ startOnLoad: true });</script>
    <style>
        body { font-family: sans-serif; margin: 2em; }
        textarea { width: 100%; height: 300px; font-family: monospace; font-size: 14px; }
        #preview { border: 1px solid #ccc; padding: 1em; margin-top: 1em; background: #f9f9f9; }
        a { color: blue; text-decoration: underline; }
        .backlinks { 
            margin-top: 2em; 
            padding: 1em; 
            background: #f0f0f0; 
            border-radius: 4px;
            border-left: 4px solid #007acc;
        }
        .backlinks h3 { 
            margin-top: 0; 
            color: #333;
            font-size: 1.1em;
        }
        .backlinks ul { 
            margin: 0.5em 0; 
            padding-left: 1.5em;
        }
        .backlinks li { 
            margin: 0.3em 0; 
        }
        .no-backlinks {
            color: #666;
            font-style: italic;
        }
    </style>
</head>
<body>
    <h1>Editing: {{ page }}</h1>
    <form method="POST">
        <textarea id="editor" name="content">{{ content }}</textarea><br>
        <button type="submit">💾 Save</button>
    </form>
    <div id="preview">{{ rendered|safe }}</div>
    
    <div class="backlinks">
        <h3>📎 Backlinks</h3>
        {% if backlinks %}
            <ul>
                {% for backlink in backlinks %}
                    <li><a href="/page/{{ backlink }}">{{ backlink }}</a></li>
                {% endfor %}
            </ul>
        {% else %}
            <p class="no-backlinks">No pages link to this page yet.</p>
        {% endif %}
    </div>

    <script>
        const textarea = document.getElementById("editor");
        const preview = document.getElementById("preview");

        textarea.addEventListener('input', () => {
            fetch("/preview", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ content: textarea.value })
            })
            .then(res => res.json())
            .then(data => {
                preview.innerHTML = data.html;
                mermaid.init(undefined, preview.querySelectorAll(".mermaid"));
            });
        });

        document.addEventListener('keydown', function(e) {
            if (e.ctrlKey && e.key === 'l') {
                e.preventDefault();
                const pageName = prompt("Link to page (e.g., MyNote):");
                if (pageName) {
                    const cursorPos = textarea.selectionStart;
                    const before = textarea.value.substring(0, cursorPos);
                    const after = textarea.value.substring(cursorPos);
                    textarea.value = before + "[[" + pageName + "]]" + after;
                    textarea.dispatchEvent(new Event('input')); // trigger preview
                }
            }
        });
    </script>
</body>
</html>
""", page=page, content=content, rendered=rendered, backlinks=backlinks)

@app.route('/preview', methods=['POST'])
def live_preview():
    data = request.get_json()
    content = data.get('content', '')
    html = render_markdown(content)
    return jsonify({'html': html})

@app.route('/pages')
def list_pages():
    """List all pages in the wiki"""
    pages = get_all_pages()
    pages.sort()
    
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
    <title>All Pages</title>
    <meta charset="utf-8">
    <style>
        body { font-family: sans-serif; margin: 2em; }
        a { color: blue; text-decoration: underline; }
        .page-list { list-style-type: none; padding: 0; }
        .page-list li { margin: 0.5em 0; padding: 0.5em; background: #f9f9f9; border-radius: 4px; }
        .page-count { color: #666; font-size: 0.9em; margin-bottom: 1em; }
    </style>
</head>
<body>
    <h1>📚 All Pages</h1>
    <div class="page-count">{{ pages|length }} pages total</div>
    
    <ul class="page-list">
        {% for page in pages %}
            <li><a href="/page/{{ page }}">{{ page }}</a></li>
        {% endfor %}
    </ul>
    
    <p><a href="/page/Home">← Back to Home</a></p>
</body>
</html>
""", pages=pages)

###################################################### Start Flask App #######################################################
port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
app.run(debug=False, host="0.0.0.0", port=port)
