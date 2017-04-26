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
        xhr.open('POST', '/index/' + indexname + '/query');
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
      var output_string = res['output']
      var output_json = JSON.parse(output_string)
      var result_class = "result-output"
      var getting_started_errors = [
        'index not found',
        'frame not found',
      ]

      if("error" in output_json) {
        result_class = "result-error"
        if(getting_started_errors.indexOf(output_json['error']) >= 0) {  
          output_string += `<br />
          <br />
          Just getting started? Try this:<br />
          $ curl -XPOST "http://127.0.0.1:10101/index/test" -d '{"options": {"columnLabel": "col"}}' # create index "test"<br />
          $ curl -XPOST "http://127.0.0.1:10101/index/test/frame/foo" -d '{"options": {"rowLabel": "row"}}' # create frame "foo"<br />
          # Select "test" in the index dropdown above<br />
          SetBit(row=0, col=0, frame=foo) # Use PQL to set a bit
          `
        }
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
                ${output_string}
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
      xhr.open('GET', '/schema')
      var select = document.getElementById('index-dropdown')

      xhr.onload = function() {
        var schema = JSON.parse(xhr.responseText)
        for(var i=0; i<schema['indexes'].length; i++) {
          var opt = document.createElement('option')
          opt.value = i+1
          opt.innerHTML = schema['indexes'][i]['name']
          select.appendChild(opt)
        }
        // set the active option to one of the populated options, instead of invalid "Index"
        if(i > 0) {
          select.value = 1;
        }
      }
      xhr.send(null)
    }

}

function setNav(e) {
  // toggle the nav buttons
  document.getElementsByClassName("nav-active")[0].classList.remove("nav-active")
  e.classList.add("nav-active")

  // toggle the main interface content divs
  document.getElementsByClassName("interface-active")[0].classList.remove("interface-active")
  name = e.id.substring(4)
  interface_el = document.getElementById('interface-' + name)
  interface_el.classList.add("interface-active")

  // hack hack
  if(name == "cluster") {
    update_cluster_status()
  }

  // toggle svgs
  // show all inactive images
  inactive_images = document.getElementsByClassName("nav-image")
  for(var i=0; i<inactive_images.length; i++) {
    inactive_images[i].classList.add("nav-image-visible")
  }
  // hide all active images
  active_images = document.getElementsByClassName("nav-image-active")
  for(var i=0; i<active_images.length; i++) {
    active_images[i].classList.remove("nav-image-visible")
  }

  for(var i=0; i<e.childNodes.length; i++) {
    var node = e.childNodes[i]
    if(node.tagName != "IMG") {
      continue
    }
    // hide inactive image on clicked element
    if(node.classList.contains("nav-image")) {
      node.classList.remove("nav-image-visible")
    }
    // show active image on clicked element
    if(node.classList.contains("nav-image-active")) {
      node.classList.add("nav-image-visible")
    }
  }

}


function update_cluster_status() {
  var xhr = new XMLHttpRequest();
  xhr.open('GET', '/status')
  status_node = document.getElementById('status')
  time_node = document.getElementById('status-time')
  xhr.onload = function() {
    status_formatted = JSON.stringify(JSON.parse(xhr.responseText), null, 4)
    status_node.innerHTML = status_formatted
    time_node.innerHTML = new Date().today() + " " + new Date().timeNow()
  }
  xhr.send(null)
}

Date.prototype.today = function () { 
    return this.getFullYear() +"/"+ (((this.getMonth()+1) < 10)?"0":"") + (this.getMonth()+1) +"/"+ ((this.getDate() < 10)?"0":"") + this.getDate();
}

Date.prototype.timeNow = function () {
     return ((this.getHours() < 10)?"0":"") + this.getHours() +":"+ ((this.getMinutes() < 10)?"0":"") + this.getMinutes() +":"+ ((this.getSeconds() < 10)?"0":"") + this.getSeconds();
}


const input = document.getElementById('query')
const output = document.getElementById('outputs')
const button = document.getElementById('query-btn')

repl = new REPL(input, output, button)
repl.populate_index_dropdown()
repl.bind_events()

input.focus()
