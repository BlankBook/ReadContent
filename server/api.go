package server

import (
    "fmt"
    "errors"
    "net/http"
    "database/sql"
    "encoding/json"

    "github.com/blankbook/shared/models"
    "github.com/blankbook/shared/web"
)

// SetupAPI adds the API routes to the provided router
func SetupAPI(r web.Router, db *sql.DB) {
    r.HandleRoute([]string{web.GET}, "/posts", GetPosts, db)
    r.HandleRoute([]string{web.GET}, "/comments", GetComments, db)
}

func GetPosts(w http.ResponseWriter, q map[string][]string, b string, db *sql.DB) {
    var err error
    defer func() {
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
    }()
    rows, err := db.Query(fmt.Sprintf("SELECT %s FROM Posts", models.PostSQLColumns))
    if err != nil {
        return
    }
    posts, err := models.GetPostsFromRows(rows)
    if err != nil {
        return
    }
    res, err := json.Marshal(posts)
    if err != nil {
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.Write(res)
}

func GetComments(w http.ResponseWriter, q map[string][]string, b string, db *sql.DB) {
    var err error
    defer func() {
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
    }()
    pPosts, hasPPost := q["parentPost"]
    pComments, hasPComment := q["parentComment"]
    var query string
    var parent string
    if (!hasPPost || len(pPosts) == 0) && (!hasPComment || len(pComments) == 0) {
        err = errors.New("Require parentPost or parentComment")
        return;
    } else if hasPComment {
        query = "SELECT %s FROM Comments WHERE ParentComment=%s"
        parent = pComments[0]
    } else {
        query = "SELECT %s FROM Comments WHERE ParentPost=%s"
        parent = pPosts[0]
    }
    rows, err := db.Query(fmt.Sprintf(query, models.CommentSQLColumns, parent))
    if err != nil {
        return
    }
    posts, err := models.GetCommentsFromRows(rows)
    if err != nil {
        return
    }
    res, err := json.Marshal(posts)
    if err != nil {
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.Write(res)
}
