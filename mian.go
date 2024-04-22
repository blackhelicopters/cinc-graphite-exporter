package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marpaia/graphite-golang"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	Name        string
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
	Environment string
}

func main() {
	ctx := context.Background()
	db, graphiteConnection := initialize(ctx)

	defer func() {
		db.Close()
		graphiteConnection.Disconnect()
	}()

	for {
		select {
		case <-time.After(time.Minute):
			nodes, notInSyncNodes := loadNodes(ctx, db)
			logAndSendNodes(nodes, notInSyncNodes, graphiteConnection)

			statusMap := scanServiceStatus()
			logAndSendServicesStatus(statusMap, graphiteConnection)
		}
	}
}

func logAndSendServicesStatus(statusMap map[string]int, gr *graphite.Graphite) {
	hostname := strings.Split(os.Getenv("HOSTNAME"), ".")[0]

	for service, status := range statusMap {
		log.Println("sent metric:", fmt.Sprintf("vlg.cinc.serving.%s.%s", hostname, service), strconv.Itoa(status))
		gr.SimpleSend(fmt.Sprintf("vlg.cinc.serving.%s.%s", hostname, service), strconv.Itoa(status))
	}
}

func initialize(ctx context.Context) (*pgxpool.Pool, *graphite.Graphite) {

	graphiteHost := os.Getenv("GRAPHITE_HOST")
	if graphiteHost == "" {
		graphiteHost = "graphite.inf.videologygroup.com"
		log.Println("Using the default GRAPHITE_HOST value:", graphiteHost)
	}
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	gr, err := graphite.NewGraphite(graphiteHost, 2003)
	if err != nil {
		log.Fatalf("Unable to connect to Graphite: %v\n", err)
	}
	return db, gr
}

func loadNodes(ctx context.Context, db *pgxpool.Pool) ([]*Node, []*Node) {
	var nodes, notInSyncNodes []*Node
	if err := pgxscan.Select(ctx, db, &nodes, `SELECT name, created_at, updated_at, environment FROM nodes`); err != nil {
		log.Fatalf("Error querying nodes from the database: %v\n", err)
	}
	if err := pgxscan.Select(ctx, db, &notInSyncNodes, `SELECT name, created_at, updated_at, environment FROM nodes WHERE updated_at <= current_timestamp - interval '60' minute`); err != nil {
		log.Fatalf("Error querying not in sync nodes from the database: %v\n", err)
	}
	return nodes, notInSyncNodes
}

func logAndSendNodes(nodes []*Node, notInSyncNodes []*Node, gr *graphite.Graphite) {
	log.Printf("Registered nodes count: %d\n", len(nodes))
	log.Printf("Not in sync node count: %d\n", len(notInSyncNodes))
	for _, n := range notInSyncNodes {
		log.Printf("Node %s was last updated more than 2 hours ago\n", n.Name)
		log.Printf("    vlg.cinc.notinsync.%s", n.Name)
		gr.SimpleSend(fmt.Sprintf("vlg.cinc.notinsync.%s", n.Name), "1")
	}
	gr.SimpleSend("vlg.cinc.nodes.registered", strconv.Itoa(len(nodes)))
	log.Println("    vlg.cinc.nodes.registered", strconv.Itoa(len(nodes)))
	gr.SimpleSend("vlg.cinc.nodes.last_updated_2_hours_ago", strconv.Itoa(len(notInSyncNodes)))
	log.Println("    vlg.cinc.nodes.last_updated_2_hours_ago", strconv.Itoa(len(notInSyncNodes)))
	log.Println("Nodes metrics have been sent")
}

func scanServiceStatus() map[string]int {
	statusMap := make(map[string]int)

	cmd := exec.Command("sudo", "cinc-server-ctl", "status")
	log.Println("sudo cinc-server-ctl status:")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Error running cinc-server-ctl status: %v\n", err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatalf("Error running cinc-server-ctl status: %v\n", err)
	}
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		service, status := parseServiceStatus(line)
		if service != "" {
			statusMap[service] = status
		}
	}
	return statusMap
}

func parseServiceStatus(line string) (string, int) {
	runRegex := regexp.MustCompile(`run: (\w+): \(pid \d+\) (\d+)s`)
	downRegex := regexp.MustCompile(`down: (\w+): (\d+)s`)
	connectedRegex := regexp.MustCompile(`run: (\w+): connected OK`)
	switch {
	case runRegex.MatchString(line):
		match := runRegex.FindStringSubmatch(line)
		service := match[1]
		return service, 1
	case downRegex.MatchString(line):
		match := downRegex.FindStringSubmatch(line)
		service := match[1]
		return service, 0
	case connectedRegex.MatchString(line):
		match := connectedRegex.FindStringSubmatch(line)
		service := match[1]
		return service, 1
	default:
		return "", -1 // Service not found or unrecognized status
	}
}
