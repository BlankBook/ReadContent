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
                  []string{"groupName"},
                  []string{"firstRank", "lastRank", "rankVersion", "ordering",
                           "firstTime", "lastTime", "maxCount"},
                  GetPosts, db)
    r.HandleRoute([]string{web.GET}, "/comments",
                  []string{"parentPost"},
                  []string{"parentComment"},
                  GetComments, db)
    // Get /contributorid gets a unique ID that must be used for all 
    // contributions the user makes to this post
    r.HandleRoute([]string{web.GET}, "/contributorid",
                  []string{"postId"}, []string{},
                  GetContributorId, db)
}

func GetPosts(w http.ResponseWriter, q map[string]string, b string, db *sql.DB) {
    if val, _ := q["ordering"]; val == timeOrderingKeyword {
        GetPostsByTime(w, q, b, db)
    } else {
        GetPostsByRank(w, q, b, db)
    }
}

type postsWithTime struct {
    Posts []models.Post
    OldestPost int64
}

func GetPostsByTime(w http.ResponseWriter, q map[string]string, b string, db *sql.DB) {
    var err error
    defer func() {
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }
    }()

    var firstTime int64 = -1
    var lastTime int64 = -1
    var maxCount = defaultMaxPostsReturned
    if q["firstTime"] != "" {
        firstTime, err = strconv.ParseInt(q["firstTime"], 10, 64)
    }
    if q["lastTime"] != "" {
        lastTime, err = strconv.ParseInt(q["lastTime"], 10, 64)
    }
    if q["maxCount"] != "" {
        maxCount, err = strconv.Atoi(q["maxCount"])
    }

    if err != nil { return }
    var posts []models.Post
    var rows *sql.Rows
    if firstTime == -1 && lastTime == -1 {
        query := `
            SELECT TOP ($1) `+models.PostSQLColumnsNewRank+` FROM Posts
            WHERE GroupName=$2
            ORDER BY Time`
        rows, err = db.Query(query, maxCount, q["groupName"])
    } else if firstTime == -1 {
        query := `
            SELECT TOP ($1) `+models.PostSQLColumnsNewRank+` FROM Posts
            WHERE GroupName=$2 AND Time <= $3
            ORDER BY Time`
        rows, err = db.Query(query, maxCount, q["groupName"], lastTime)
    } else if lastTime == -1 {
        query := `
            SELECT TOP ($1) `+models.PostSQLColumnsNewRank+` FROM Posts
            WHERE GroupName=$2 AND $3 <= Time
            ORDER BY Time`
        rows, err = db.Query(query, maxCount, q["groupName"], firstTime)
    } else {
        query := `
            SELECT TOP ($1) `+models.PostSQLColumnsNewRank+` FROM Posts
            WHERE GroupName=$2 AND $3 <= Time AND Time <= $4
            ORDER BY Time`
        rows, err = db.Query(query, maxCount, q["groupName"], firstTime, lastTime)
    }
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

func GetPostsByRank(w http.ResponseWriter, q map[string]string, b string, db *sql.DB) {
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
    if q["firstRank"] != "" {
        firstRank, err = strconv.ParseInt(q["firstRank"], 10, 64)
    }
    if q["lastRank"] == "" {
        lastRank = firstRank + defaultMaxPostsReturned
    } else {
        lastRank, err = strconv.ParseInt(q["lastRank"], 10, 64)
    }
    if q["rankVersion"] != "" {
        rankVersion, err = strconv.ParseInt(q["rankVersion"], 10, 64)
    }
    if q["maxCount"] != "" {
        maxCount, err = strconv.Atoi(q["maxCount"])
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
        rows, err = db.Query(query, maxCount, q["groupName"], firstRank, lastRank, rankVersion)
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
