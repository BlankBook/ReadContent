package server

import (
    "fmt"
    "strconv"
    "net/http"
    "database/sql"
    "encoding/json"

    "github.com/blankbook/shared/models"
    "github.com/blankbook/shared/web"
)

const defaultMaxPostsReturned = 1000
const timeOrderingKeyword = "time"
const rankOrderingKeyword = "rank"

// SetupAPI adds the API routes to the provided router
func SetupAPI(r web.Router, db *sql.DB) {
    r.HandleRoute([]string{web.GET}, "/posts",
                  []string{"groupname"},
                  []string{"firstrank", "lastrank", "rankversion", "ordering",
                           "firsttime", "lasttime", "maxcount"},
                  GetPosts, db)
    r.HandleRoute([]string{web.GET}, "/comments",
                  []string{"parentpost"},
                  []string{"parentcomment", "ordering"},
                  GetComments, db)
    // Get /contributorid gets a unique ID that must be used for all 
    // contributions the user makes to this post
    r.HandleRoute([]string{web.GET}, "/contributorid",
                  []string{"postid"}, []string{},
                  GetContributorId, db)
    r.HandleRoute([]string{web.GET}, "/health",
                  []string{}, []string{},
                  GetHealth, db)
}

func GetPosts(w http.ResponseWriter, q map[string][]string, b string, db *sql.DB) {
    if v, ok := q["ordering"]; ok && v[0] == timeOrderingKeyword {
        GetPostsByTime(w, q, b, db)
    } else {
        GetPostsByRank(w, q, b, db)
    }
}

type postsWithTime struct {
    Posts []models.Post
    OldestPost int64
}

func GetPostsByTime(w http.ResponseWriter, q map[string][]string, b string, db *sql.DB) {
    var err error
    defer func() {
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
    }()

    var firstTime int64 = -1
    var lastTime int64 = -1
    var maxCount = defaultMaxPostsReturned
    if v, ok := q["firsttime"]; ok {
        firstTime, err = strconv.ParseInt(v[0], 10, 64)
    }
    if v, ok := q["lasttime"]; ok {
        lastTime, err = strconv.ParseInt(v[0], 10, 64)
    }
    if v, ok := q["maxcount"]; ok {
        maxCount, err = strconv.Atoi(v[0])
    }

    if err != nil { return }
    var posts []models.Post
    var rows *sql.Rows
    query := "SELECT TOP ($1) "+models.PostSQLColumnsNewRank+" FROM Posts "
    args := []interface{}{maxCount}
    if firstTime == -1 && lastTime == -1 {
        query += "WHERE GroupName=$2 "
        args = append(args, q["groupname"][0])
    } else if firstTime == -1 {
        query += "WHERE GroupName=$2 AND Time <= $3 "
        args = append(args, q["groupname"][0], lastTime)
    } else if lastTime == -1 {
        query += "WHERE GroupName=$2 AND $3 <= Time "
        args = append(args, q["groupname"][0], firstTime)
    } else {
        query += "WHERE GroupName=$2 AND $3 <= Time AND Time <= $4 "
        args = append(args, q["groupname"][0], firstTime, lastTime)
    }
    query += "ORDER BY Time DESC"
    rows, err = db.Query(query, args...)
    if err == nil {
        posts, err = models.GetPostsFromRows(rows)
    }

    if err != nil { return }
    res, err := json.Marshal(postsWithTime{posts, firstTime})
    if err != nil { return }
    w.Header().Set("Content-Type", "application/json")
    w.Write(res)
}


type postsWithVersion struct {
    Posts []models.Post
    RankVersion int64
}

func GetPostsByRank(w http.ResponseWriter, q map[string][]string, b string, db *sql.DB) {
    var err error
    defer func() {
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
    }()

    var firstRank int64 = 0
    var lastRank int64
    var rankVersion int64 = -1
    var maxCount = defaultMaxPostsReturned
    if v, ok := q["firstrank"]; ok {
        firstRank, err = strconv.ParseInt(v[0], 10, 64)
    } 
    if v, ok := q["lastrank"]; ok {
        lastRank, err = strconv.ParseInt(v[0], 10, 64)
    } else {
        lastRank = firstRank + defaultMaxPostsReturned
    }
    if v, ok := q["rankversion"]; ok {
        rankVersion, err = strconv.ParseInt(v[0], 10, 64)
    }
    if v, ok := q["maxcount"]; ok {
        maxCount, err = strconv.Atoi(v[0])
    }
    if err != nil { return }

    var posts []models.Post
    gotRows := make(chan bool)
    gotVers := make(chan bool)
    go func() {
        var rows *sql.Rows
        query := `
            DECLARE @LatestRankVersion BIGINT
            SET @LatestRankVersion = (SELECT RankVersion FROM State)
            IF ($5=@LatestRankVersion OR $5=-1)
            BEGIN
                SELECT TOP ($1) `+models.PostSQLColumnsNewRank+` FROM Posts
                WHERE GroupName=$2 AND Rank >= $3 AND Rank <= $4
                ORDER BY Rank
            END
            ELSE
            BEGIN
                SELECT TOP ($1) `+models.PostSQLColumnsOldRank+` FROM Posts
                WHERE GroupName=$2 AND OldRank >= $3 AND OldRank <= $4
                ORDER BY OldRank
            END`
        rows, err = db.Query(query, maxCount, q["groupname"][0], firstRank, lastRank, rankVersion)
        if err == nil {
            posts, err = models.GetPostsFromRows(rows)
        }
        gotRows <- true
    }()
    go func() {
        var rows *sql.Rows
        query := `SELECT RankVersion FROM State`
        rows, err = db.Query(query)
        defer rows.Close()
        rows.Next()
        if err == nil {
            var publicRankVersion int64
            err = rows.Scan(&publicRankVersion)
            // Determine the actual version of rankings we are sending to
            // the client - it is either the latest public version, if
            // the client is requesting a higher rank or has not specified it,
            // or one less if the client wants an older version of the rankings
            if rankVersion == -1 || rankVersion > publicRankVersion {
                rankVersion = publicRankVersion
            } else {
                rankVersion = publicRankVersion - 1
            }
        }
        gotVers <- true
    }()
    <-gotVers
    <-gotRows

    if err != nil { return }
    res, err := json.Marshal(postsWithVersion{posts, rankVersion})
    if err != nil { return }
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

    parentPost := q["parentpost"][0]
    parentComment := q["parentcomment"][0]
    ordering := "ORDER BY Score DESC, Time DESC"
    if q["ordering"][0] == timeOrderingKeyword {
        ordering = "ORDER BY Time DESC, Score DESC"
    }

    var rows *sql.Rows
    if parentComment != "" {
        query := `SELECT `+models.CommentSQLColumns+` FROM Comments
            WHERE ParentComment=$1 AND ParentPost=$2 `+ordering
        fmt.Printf(query)
        rows, err = db.Query(query, parentComment, parentPost)
    } else {
        query := `SELECT `+models.CommentSQLColumns+`
            FROM Comments WHERE ParentPost=$1 `+ordering
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

func GetContributorId(w http.ResponseWriter, q map[string][]string, b string, db *sql.DB) {
    query := `
        UPDATE TOP (1) Posts
        SET NextUserID = NextUserID + 1
        OUTPUT INSERTED.NextUserID
        WHERE ID = $1
       `
    idrow, err := db.Query(query, q["postid"][0])
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

func GetHealth(w http.ResponseWriter, q map[string][]string, b string, db *sql.DB) {
    w.WriteHeader(http.StatusOK)
}
