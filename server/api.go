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
    r.HandleRoute([]string{web.GET}, "/posts",
                  []string{}, []string{},
                  GetPosts, db)
    r.HandleRoute([]string{web.GET}, "/comments",
                  []string{"parentPost"}, []string{"parentComment"},
                  GetComments, db)
    // Get /contributorid gets a unique ID that must be used for all 
    // contributions the user makes to this post
    r.HandleRoute([]string{web.GET}, "/contributorid",
                  []string{"postId"}, []string{},
                  GetContributorId, db)
}

func GetPosts(w http.ResponseWriter, q map[string]string, b string, db *sql.DB) {
    var err error
    defer func() {
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
    }()
    query := "SELECT " + models.PostSQLColumns + " FROM Posts"
    rows, err := db.Query(query)
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

func GetComments(w http.ResponseWriter, q map[string]string, b string, db *sql.DB) {
    var err error
    defer func() {
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
    }()
    parentPost := q["parentPost"]
    parentComment := q["parentComment"]
    var rows *sql.Rows
    if parentComment != "" {
        query := "SELECT " + models.CommentSQLColumns +
                 " FROM Comments WHERE ParentComment=$1 AND ParentPost=$2"
        rows, err = db.Query(query, parentComment, parentPost)
    } else {
        query := "SELECT " + models.CommentSQLColumns +
                 " FROM Comments WHERE ParentPost=$1"
        rows, err = db.Query(query, parentPost)
    }
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

func GetContributorId(w http.ResponseWriter, q map[string]string, b string, db *sql.DB) {
    query := `
        UPDATE TOP (1) Posts
        SET NextUserID = NextUserID + 1
        OUTPUT INSERTED.NextUserID
        WHERE ID = $1
       `
    idrow, err := db.Query(query, q["postId"])
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    var id int
    idrow.Next()
    err = idrow.Scan(&id)
    idrow.Close()
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    res := fmt.Sprintf(`{"id":%d}`, id)
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(res))
}
