from flask import Flask, request, redirect, url_for, render_template_string, jsonify
import os
import markdown2
import re

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
    </style>
</head>
<body>
    <h1>Editing: {{ page }}</h1>
    <form method="POST">
        <textarea id="editor" name="content">{{ content }}</textarea><br>
        <button type="submit">💾 Save</button>
    </form>
    <div id="preview">{{ rendered|safe }}</div>

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
""", page=page, content=content, rendered=rendered)

@app.route('/preview', methods=['POST'])
def live_preview():
    data = request.get_json()
    content = data.get('content', '')
    html = render_markdown(content)
    return jsonify({'html': html})



###################################################### Start Flask App #######################################################
port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
app.run(debug=False, host="0.0.0.0", port=port)
