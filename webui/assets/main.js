class REPL {
    constructor(input, output, button) {
        this.input = input
        this.output = output
        this.button = button
        this.history = []
        this.history_index = 0
        this.history_buffer = ''
        this.result_number = 0
    }
    bind_events() {
        const repl = this
        const keys = {
          ENTER: 13,
          UP_ARROW: 38,
          DOWN_ARROW: 40
        }

        this.input.addEventListener("keydown", (e) => {
          if (e.keyCode == keys.UP_ARROW) {
            e.preventDefault()
            if (this.input.value.substring(0, this.input.selectionStart).indexOf('\n') == '-1') {
                if (this.history_index == 0) {
                    return
                } else {
                    if (this.history_index == this.history.length) {
                        this.history_buffer = this.input.value
                    }
                    this.history_index--
                    this.input.value = this.history[this.history_index]
                    this.input.setSelectionRange(this.input.value.length, this.input.value.length)
                }
            }
          }
          if (e.keyCode == keys.DOWN_ARROW) {
            e.preventDefault()
            if (this.input.value.substring(this.input.selectionEnd, this.input.length).indexOf('\n') == '-1') {
                if (this.history_index == this.history.length) {
                    return
                } else {
                    this.history_index++
                    if (this.history_index == this.history.length) {
                        this.input.value = this.history_buffer
                    } else {
                        this.input.value = this.history[this.history_index]
                    }
                    this.input.setSelectionRange(this.input.value.length, this.input.value.length)
                }
            }
          }
          if (e.keyCode == keys.ENTER && !e.shiftKey) {
            e.preventDefault()
            this.submit();
          }
        })
        this.button.onclick = function() {
            repl.submit();
        };
    }

    submit() {
        this.history_buffer = ''
        this.history_index = this.history.length
        this.history[this.history_index] = this.input.value
        this.history_index++
        this.process_query(this.input.value)
        this.input.value = ""
    }

    process_query(query) {
        var xhr = new XMLHttpRequest();
        var e = document.getElementById("index-dropdown");
        var indexname = e.options[e.selectedIndex].text;
        xhr.open('POST', '/db/' + indexname + '/query');  // TODO db->index
        xhr.setRequestHeader('Content-Type', 'application/text');

        const repl = this
        var start_time = new Date().getTime();
        xhr.send(query)
        xhr.onload = function() {
            var end_time = new Date().getTime()
            repl.result_number++
            repl.createSingleOutput({
              "input": query, 
              "output": xhr.responseText, 
              "indexname": indexname,
              "querytime_ms": end_time - start_time,
            })
        }

    }

    createSingleOutput(res) {
      var node = document.createElement("div");
      node.classList.add('output');
      var result_class = "result-output"
      var output = JSON.parse(res['output'])
      if("error" in output) {
        result_class = "result-error"
      }
      var markup =`
        <div  class="panes">
          <div class="pane active">
            <div class="result-io">
              <div class="result-io-header">
                <h5>Input</h5>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                <em>Source: ${res.indexname}</em>
              </div>
              <div class="result-input">
                ${res.input}
              </div>
            </div>
            <div class="result-io">
              <div class="result-io-header">
                <h5>output</h5>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                <em>${res.querytime_ms} ms</em>
              </div>
              <div class="${result_class}">
                ${res.output}
              </div>
            </div>
          </div>
          <div class="pane">
            <div class="raw">
    
              <a href="${res.url}">Export raw .csv</a>
            </div>
          </div>
        </div>
      `
      node.innerHTML = markup;
      this.output.insertBefore(node, this.output.firstChild)
    }

    populate_index_dropdown() {
      var xhr = new XMLHttpRequest();
      xhr.open('GET', '/schema')  // TODO schema->status (?)
      var select = document.getElementById('index-dropdown')

      xhr.onload = function() {
        var schema = JSON.parse(xhr.responseText)
        for(var i=0; i<schema['dbs'].length; i++) { // TODO db->index
          var opt = document.createElement('option')
          opt.innerHTML = schema['dbs'][i]['name']
          select.appendChild(opt)
        }
      }
      xhr.send(null)
    }

}


const input = document.getElementById('query')
const output = document.getElementById('outputs')
const button = document.getElementById('query-btn')

repl = new REPL(input, output, button)
repl.populate_index_dropdown()
repl.bind_events()