fs=require('fs')
var books = JSON.parse(fs.readFileSync('C18thBooks.json'))

Object.keys(books.values).forEach((title)=>{
  var book = books.values[title]
  Object.keys(book.fbtee).forEach ((id) => {
    book.fbtee[id].authors = book.fbtee[id].authors.map((a)=>{
      m=/(.+), (.+)/.exec(a);
      if (m)
        return (m[2]+" "+m[1])
        .replace(/[\[\]]/g,'').replace(/d' /g,"d'")
      else
        return a;
    })
  })
})

console.log (JSON.stringify (books, null, 2))
