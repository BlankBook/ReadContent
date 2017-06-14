package server

import (
    "fmt"
    "net/http"
    "database/sql"
    "encoding/json"

    "github.com/blankbook/shared/models"
    "github.com/blankbook/shared/web"
)

// SetupAPI adds the API routes to the provided router
func SetupAPI(r web.Router, db *sql.DB) {
    r.HandleRoute([]string{web.GET}, "/posts", GetPosts, db)
}

func GetPosts(w http.ResponseWriter, queryParams map[string][]string, body string, db *sql.DB) {
    var err error
    defer func() {
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
    }()
    rows, err := db.Query(fmt.Sprintf("SELECT %s FROM posts", models.PostSQLColumns))
    if err != nil {
        return
    }
    posts, err := models.GetPostsFromRows(rows)
    if err != nil {
        return
    }
    b, err := json.Marshal(posts)
    if err != nil {
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.Write(b)
}
