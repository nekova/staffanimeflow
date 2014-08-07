package animes

import (
    "html/template"
    "net/http"
    "fmt"
    "code.google.com/p/goauth2/appengine/serviceaccount"
    "code.google.com/p/google-api-go-client/bigquery/v2"
    "appengine"
)

const (
    PROJECT_ID string = "staffanimeflow"
    DATASET_ID string = "anime"
    TABLE_ID   string = "staff"
)

func init() {
    http.HandleFunc("/", handler)
    http.HandleFunc("/search", QueryHandler)
}

const templateHTML = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>animes</title>
</head>
<body>
</body>
</html>
`

func handler(w http.ResponseWriter, r *http.Request) {
    result := r.FormValue("name")
    htmltemplate := template.Must(template.New("html").Parse(templateHTML))
    var err = htmltemplate.Execute(w, result)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
}

func QueryHandler(rw http.ResponseWriter, req *http.Request) {
    query := req.FormValue("name")
    c := appengine.NewContext(req)
    client, err := serviceaccount.NewClient(c, bigquery.BigqueryScope)
    if err != nil {
        fmt.Fprintf(rw, "%s", err.Error())
        return
    }

    service, err := bigquery.New(client)
    if err != nil {
        fmt.Fprintf(rw, "%s", err.Error())
        return
    }

    response, err := service.Jobs.Query(PROJECT_ID, &bigquery.QueryRequest{
        Kind:  "bigquery#queryRequest",
        Query: "SELECT * FROM " + DATASET_ID + "." + TABLE_ID + " WHERE staff CONTAINS \"" + query + "\" LIMIT 100",
    }).Do()
    if err != nil {
        fmt.Fprintf(rw, "%s", err.Error())
        return
    }

    animes := response.Schema.Fields
    for _, row := range response.Rows {
        for i, cell := range row.F {
            if cell.V != nil && animes[i].Name != "staff" {
                fmt.Fprintf(rw,"%v: %v", animes[i].Name, cell.V)
            }
        }
    }
}
