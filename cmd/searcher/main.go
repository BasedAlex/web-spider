package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		startHttpServer(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		search(ctx)
		cancel()
	}()
	wg.Wait()
	
}

func startHttpServer(ctx context.Context) {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/healthz", func (w http.ResponseWriter, r *http.Request){
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr: ":3000",
		Handler: r,
	}
	log.Println("Server started")
	go func() {
		<- ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		srv.Shutdown(ctx)
	}()
	srv.ListenAndServe()
}

func search(ctx context.Context) {
	urls := []string{"https://hltv.org", "https://wikipedia.org"}

	conn, err := kafka.DialLeader(ctx, "tcp", "localhost:9092", "topic_test", 0)
	if err != nil {
		log.Println(err)
		return
	}

	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	ticker := time.NewTicker(time.Minute)

	defer ticker.Stop()
	log.Println("Searcher started")
	for {
		select {
		case <- ticker.C:
			for _, url := range urls {
				log.Println(url)
				conn.WriteMessages(kafka.Message{Value: []byte(url)})
			}
		case <- ctx.Done():
			log.Println(ctx.Err())
			return
		}
	}

}