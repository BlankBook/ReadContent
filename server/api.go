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
    r.HandleRoute([]string{web.GET}, "/comments", GetComments, db)
    // Get /contributorid gets a unique ID that must be used for all 
    // contributions the user makes to this post
    r.HandleRoute([]string{web.GET}, "/contributorid", GetContributorId, db)
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
    if !hasPPost || len(pPosts) == 0 {
        http.Error(w, "Query parameter parentPost required", http.StatusBadRequest)
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

func GetContributorId(w http.ResponseWriter, q map[string][]string, b string, db *sql.DB) {
    postIds, hasPostId := q["postId"]
    if !hasPostId || len(postIds) == 0 {
        http.Error(w, "Query parameter postId required", http.StatusBadRequest)
        return
    }
    query := `
        UPDATE TOP (1) Posts
        SET NextUserID = NextUserID + 1
        OUTPUT INSERTED.NextUserID
        WHERE ID = %s
       `
    idrow, err := db.Query(fmt.Sprintf(query, postIds[0]))
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
