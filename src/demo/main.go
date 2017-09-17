// 1. add log file
// 2. log level
// 2. add log rotation
// 3. add line number
// 5. send message: tcp
// 6. error handling
// 7. panic handle
// 8. how to test code
// 9. support file filter: exclude

package main

import (
	"encoding/gob"
	"encoding/json"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"time"
)

type Config struct {
	Cool   int      `json:"cool"`
	Tcp    string   `json:"tcp,omitempty"`
	Udp    string   `json:"udp,omitempty"`
	Conts  []string `json:"container"`
	Dam    string   `json:"dam"`
	Tenant string   `json:"tenant"`
	User   string   `json:"user"`
	Pass   string   `json:"pass"`
	Samba  string   `json:"samba"`
	Gap    int      `json:"gap"`
}

// file status
const (
	No    int = 0
	New   int = 1
	Ready int = 2
)

// start and end
const (
	WaitStart   string = "1"
	StreamStart string = "2"
	StreamDone  string = "12"
	UploadStart string = "3"
	UploadDone  string = "13"
	SyncStart   string = "4"
	SyncDone    string = "14"
	TailStart   string = "5"
	TailEnd     string = "15"
	SymErr      string = "20"
	UploadErr   string = "21"
	SyncErr     string = "22"
)

const ConfPath string = "/etc/demo/demo.conf"

var msgInfo = make(map[string]string)
var config Config

var enable bool = false
var isDataVary = false

func init() {
	// load configration
	var err error
	config, err = loadConf()
	if err != nil {
		log.Println("error:", err)
		return
	}

	// TODO: verify configuration
	if len(config.Conts) == 0 {
		log.Println("error: no container available")
		return
	}

	log.Println("configration:", config)

	// generate msgInfo
	msgInfo[WaitStart] = "start to wait"
	msgInfo[StreamStart] = "data varying"
	msgInfo[StreamDone] = "data invariant"
	msgInfo[UploadStart] = "upload start"
	msgInfo[UploadDone] = "upload done"
	msgInfo[SyncStart] = "sync start"
	msgInfo[SyncDone] = "sync done"
	msgInfo[TailStart] = "start tail work of show"
	msgInfo[TailEnd] = "end tail work of show"
	msgInfo[SymErr] = "system error"
	msgInfo[UploadErr] = "upload error"
	msgInfo[SyncErr] = "sync error"
}

func monitor(done <-chan bool, chReq chan<- Request) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	log.Println("watch file: ", config.Samba)
	err = watcher.Add(config.Samba)
	if err != nil {
		log.Fatal(err)
	}

	pending := make(map[string]int)
	var waitTime int

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				waitTime = 0

				// send the signal once
				if !isDataVary {
					if event.Op&fsnotify.Remove != fsnotify.Remove {
						udpSender(StreamStart)
					}
					isDataVary = true
				}

				if event.Op&fsnotify.Create == fsnotify.Create {
					enable = true
					log.Println("event:", event)
					pending[event.Name] = New
					//log.Println("pending:", pending)
				}

				if event.Op&fsnotify.Remove == fsnotify.Remove {
					log.Println("event:", event)
					pending[event.Name] = No
					//log.Println("pending:", pending)
				}

				if event.Op&fsnotify.Chmod == fsnotify.Chmod {
					enable = true
					log.Println("event:", event)
					pending[event.Name] = Ready
					//log.Println("pending:", pending)
				}

			case err := <-watcher.Errors:
				waitTime = 0
				if err != nil {
					udpSender(SymErr)
					log.Println("error:", err)
				}

			case <-time.After(time.Second):
				waitTime += 1
				isDataVary = false
				//log.Println("cool waitTime:", waitTime)

				// add 'enable' to prevent invalid signal,
				// when starting program
				/*
					if waitTime == 1 && enable {
						udpSender(StreamDone)
					}
				*/

				// check length of pending to prevent invalid signal
				if waitTime == 1+config.Gap && len(pending) > 0 {
					udpSender(StreamDone)
					log.Println("cold files:", pending)

					req := Request{Level: 0}
					for k, v := range pending {
						if v != No {
							req.Files = append(req.Files, k)
						}
						delete(pending, k)
					}

					if len(req.Files) > 0 {
						log.Println("send request:", req)
						chReq <- req
					}
				}

				// prevent waitTime to be too large
				if waitTime >= 2000 {
					// the reset value should be larger than
					// condition in which wait signal sended
					waitTime = 2 + config.Gap
				}

			case <-done:
				log.Println("done")
				return
			}
		}
	}()

	<-done
}

func genConf() error {
	conf := Config{
		Cool:   10,
		Tcp:    "localhost:1234",
		Udp:    "localhost:1234",
		Conts:  []string{"hello", "test"},
		Dam:    "10.2.162.110",
		Tenant: "da",
		User:   "system",
		Pass:   "123456",
		Samba:  "/tmp/foo",
		Gap:    3,
	}

	b, err := json.MarshalIndent(conf, "", "    ")
	if err != nil {
		log.Println("error:", err)
		return err
	}

	os.Stdout.Write(b)
	return nil
}

func loadConf() (Config, error) {
	log.Printf("config file: %s", ConfPath)

	if _, err := os.Stat(ConfPath); os.IsNotExist(err) {
		log.Println("file not exist:", ConfPath)
		return Config{}, err
	}

	bytes, err := ioutil.ReadFile(ConfPath)
	if err != nil {
		log.Println("error:", err)
		return Config{}, err
	}

	var config Config
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		log.Println("error:", err)
		return Config{}, err
	}

	return config, nil
}

func udpSender(msg string) error {
	log.Println("send msg via udp:", msg, msgInfo[msg])

	conn, err := net.Dial("udp", config.Udp)
	if err != nil {
		log.Println("connect error:", err)
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte(msg))
	if err != nil {
		log.Println("send error:", err)
	}
	/*
		// the length of data sended to windows is 4.
		// but the length should be 1;
		err = gob.NewEncoder(conn).Encode(msg)
		if err != nil {
			log.Println("send error:", err)
			return err
		}
	*/

	return nil
}

func tcpSender(msg string) error {
	log.Println("send msg via tcp:", msg)

	conn, err := net.Dial("tcp", config.Tcp)
	if err != nil {
		log.Println("connect error:", err)
		return err
	}
	defer conn.Close()

	err = gob.NewEncoder(conn).Encode(msg)
	if err != nil {
		log.Println("send error:", err)
		return err
	}

	return nil
}

type Request struct {
	Level int
	Files []string // file name
}

func selectCont(lastCont string) string {
	// verify configuration
	if lastCont == "" {
		return config.Conts[0]
	}

	var newCont string
	for _, value := range config.Conts {
		// keep two different containers in config file
		if lastCont != value {
			newCont = value
			break
		}
	}
	return newCont
}

func cmdExecutor(name string, arg ...string) error {
	log.Println("cmd:", name, ", options:", arg)

	out, err := exec.Command(name, arg...).Output()
	if err != nil {
		log.Println("error:", err)
		return err
	}

	log.Println("result:", string(out))
	return nil
}

func upload(file, cont string) error {
	log.Printf("upload file %s to container %s", file, cont)

	name := "dacli"
	args := []string{
		"putObject",
		"-p", config.Dam,
		"-t", config.Tenant,
		"-u", config.User,
		"-P", config.Pass,
		"-c", cont,
		"-f", file,
		"-o", file,
		"--xdata", "use=demo",
	}

	err := cmdExecutor(name, args...)
	if err != nil {
		return err
	}

	return nil
}

func sync(cont string) error {
	log.Printf("sync container: %s", cont)

	name := "dacli"
	args := []string{
		"sync",
		"-p", config.Dam,
		"-t", config.Tenant,
		"-u", config.User,
		"-P", config.Pass,
		"-c", cont,
		"--sync",
	}

	err := cmdExecutor(name, args...)
	if err != nil {
		return err
	}

	return nil
}

func handler(done <-chan bool, chReq <-chan Request) {
	log.Println("start handler to handle request")

	var cont string

	for {
		select {
		case req := <-chReq:
			log.Println("receive req:", req)

			// 1. select contaienr
			cont = selectCont(cont)
			log.Println("select container:", cont)

			// 2. upload files as a batch
			udpSender(UploadStart)
			time.Sleep(time.Duration(config.Cool) * time.Second)
			for _, file := range req.Files {
				err := upload(file, cont)
				if err != nil {
					log.Println("fail to upload file:", file, ", to:", cont)
				}
			}
			time.Sleep(time.Duration(config.Gap) * time.Second)
			udpSender(UploadDone)

			// 3. sync
			udpSender(SyncStart)
			err := sync(cont)
			if err != nil {
				udpSender(SyncErr)
			} else {
				udpSender(SyncDone)
			}

			// 4. tail of show
			udpSender(TailStart)
			time.Sleep(time.Duration(config.Gap) * time.Second)
			if !isDataVary {
				udpSender(TailEnd)
				udpSender(WaitStart)
			}

		case <-done:
			log.Println("done")
			return
		}
	}
}

func main() {
	done := make(chan bool)
	chReq := make(chan Request)

	// start monitor: generate request, send to handler;
	go monitor(done, chReq)

	// start handler: handle request
	go handler(done, chReq)

	<-done
}
