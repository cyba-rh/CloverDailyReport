from flask import Flask, render_template_string, request
import re

app = Flask(__name__)

HTML = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Quote Numbers Tool</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        textarea { width: 100%; height: 260px; font-size: 1em; }
        .container { max-width: 700px; margin: auto; }
        button { padding: 8px 20px; font-size: 1em; margin-top: 10px; }
        label { font-weight: bold; }
        .options { margin: 10px 0; }
        .button-row { display: flex; gap: 10px; }
        .inline { display: inline-block; margin-right: 15px; }
        .digits-box { width: 60px; }
    </style>
    <script>
    function exclusiveCheckbox(clickedId) {
        var remove = document.getElementById('remove_leading_zeroes');
        var pad = document.getElementById('pad_leading_zeroes');
        if(clickedId === 'remove_leading_zeroes' && remove.checked) {
            pad.checked = false;
        } else if(clickedId === 'pad_leading_zeroes' && pad.checked) {
            remove.checked = false;
        }
    }
    </script>
</head>
<body>
<div class="container">
    <h2>Quote Numbers Tool</h2>
    <form method="post">
        <label for="input_numbers">Input (any separator: comma, space, newline, semicolon, tab):</label><br>
        <textarea id="input_numbers" name="input_numbers">{{ input_numbers }}</textarea><br>
        <div class="options">
            <span class="inline"><input type="checkbox" id="remove_leading_zeroes" name="remove_leading_zeroes" value="1" {% if remove_leading_zeroes %}checked{% endif %} onclick="exclusiveCheckbox('remove_leading_zeroes')"> <label for="remove_leading_zeroes">Remove leading zeroes</label></span>
            <span class="inline"><input type="checkbox" id="pad_leading_zeroes" name="pad_leading_zeroes" value="1" {% if pad_leading_zeroes %}checked{% endif %} onclick="exclusiveCheckbox('pad_leading_zeroes')"> <label for="pad_leading_zeroes">Pad leading zeroes</label></span>
            <span class="inline">Total digits: <input type="number" min="1" max="50" class="digits-box" name="total_digits" value="{{ total_digits }}"></span>
        </div>
        <div class="options">
            <label><input type="radio" name="output_mode" value="quoted" {% if output_mode == 'quoted' %}checked{% endif %}> Comma-separated with quotes</label>
            <label><input type="radio" name="output_mode" value="plain" {% if output_mode == 'plain' %}checked{% endif %}> Comma-separated (no quotes)</label>
            <label><input type="radio" name="output_mode" value="bracketed" {% if output_mode == 'bracketed' %}checked{% endif %}> Parentheses (n1), (n2), ...</label>
        </div>
        <div class="button-row">
            <button type="submit" name="action" value="convert">Convert</button>
            <button type="submit" name="action" value="clear">Clear</button>
        </div>
    </form>
    {% if output %}
    <label for="output">Output:</label><br>
    <textarea id="output" readonly>{{ output }}</textarea>
    {% endif %}
</div>
</body>
</html>
'''

def parse_numbers(input_string):
    # Split on comma, semicolon, whitespace, or newline
    return [n.strip() for n in re.split(r'[\s,;\t]+', input_string) if n.strip()]

def process_number(n, remove_leading_zeroes, pad_leading_zeroes, total_digits):
    if remove_leading_zeroes:
        n = n.lstrip('0') or '0'
    if pad_leading_zeroes:
        n = n.zfill(total_digits)
    return n

def format_numbers(numbers, mode, remove_leading_zeroes, pad_leading_zeroes, total_digits):
    processed = [process_number(n, remove_leading_zeroes, pad_leading_zeroes, total_digits) for n in numbers]
    if mode == 'quoted':
        return ', '.join([f"'{n}'" for n in processed])
    elif mode == 'bracketed':
        return ', '.join([f"('{n}')" for n in processed])
    else:
        return ', '.join(processed)

@app.route('/', methods=['GET', 'POST'])
def index():
    input_numbers = ''
    output = ''
    output_mode = 'quoted'
    remove_leading_zeroes = False
    pad_leading_zeroes = False
    total_digits = 12
    if request.method == 'POST':
        action = request.form.get('action', 'convert')
        if action == 'clear':
            input_numbers = ''
            output = ''
            output_mode = 'quoted'
            remove_leading_zeroes = False
            pad_leading_zeroes = False
            total_digits = 12
        else:
            input_numbers = request.form.get('input_numbers', '')
            output_mode = request.form.get('output_mode', 'quoted')
            remove_leading_zeroes = request.form.get('remove_leading_zeroes') == '1'
            pad_leading_zeroes = request.form.get('pad_leading_zeroes') == '1'
            try:
                total_digits = int(request.form.get('total_digits', 12))
            except Exception:
                total_digits = 12
            numbers = parse_numbers(input_numbers)
            output = format_numbers(numbers, output_mode, remove_leading_zeroes, pad_leading_zeroes, total_digits)
    return render_template_string(HTML, input_numbers=input_numbers, output=output, output_mode=output_mode, remove_leading_zeroes=remove_leading_zeroes, pad_leading_zeroes=pad_leading_zeroes, total_digits=total_digits)

if __name__ == '__main__':
    app.run(debug=False, use_reloader=False, port=5123)