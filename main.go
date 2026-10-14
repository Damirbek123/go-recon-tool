package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

type Result struct {
	Port int
	Open bool
}

func worker(target string, jobs <-chan int, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for port := range jobs {
		address := fmt.Sprintf("%s:%d", target, port)
		conn, err := net.DialTimeout("tcp", address, 1*time.Second)

		if err != nil {
			results <- Result{Port: port, Open: false}
			continue
		}

		conn.Close()
		results <- Result{Port: port, Open: true}
	}
}

func runScanner(target string, startPort, endPort, threads int) {
	fmt.Printf("[*] Начинаем сканирование %s (порты %d-%d)...\n", target, startPort, endPort)
	startTime := time.Now()

	numPorts := endPort - startPort + 1
	jobs := make(chan int, numPorts)
	results := make(chan Result, numPorts)
	var wg sync.WaitGroup

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go worker(target, jobs, results, &wg)
	}

	for port := startPort; port <= endPort; port++ {
		jobs <- port
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	openPorts := 0
	for res := range results {
		if res.Open {
			fmt.Printf("[+] Порт %d/tcp ОТКРЫТ!\n", res.Port)
			openPorts++
		}
	}

	fmt.Printf("\n[+] Готово! Найдено открытых портов: %d. Время: %v\n", openPorts, time.Since(startTime))
}

func main() {
	var target string
	var startPort int
	var endPort int
	var threads int

	var rootCmd = &cobra.Command{
		Use:   "recon",
		Short: "Быстрый сканер портов",
		Run: func(cmd *cobra.Command, args []string) {
			if target == "" {
				fmt.Println("[!] Ошибка: Укажите цель через -t")
				os.Exit(1)
			}
			runScanner(target, startPort, endPort, threads)
		},
	}

	rootCmd.Flags().StringVarP(&target, "target", "t", "", "Цель (обязательно)")
	rootCmd.Flags().IntVarP(&startPort, "start", "s", 1, "Стартовый порт")
	rootCmd.Flags().IntVarP(&endPort, "end", "e", 1024, "Конечный порт")
	rootCmd.Flags().IntVarP(&threads, "threads", "w", 100, "Потоки")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}