package main


import (
	"fmt"
	"os"
	"time"
        "log" 

	"github.com/nats-io/nats.go"
)

func main() {
   if len(os.Args) < 4 {fmt.Printf("Run this NATS pipe like \n%v input_subj output_subj log_subj\n",os.Args[0])
                        os.Exit(1)
                       }

   pipe(os.Args[1]+".*",os.Args[2],os.Args[3])
}

func pipe(first string, next string, logsubj string) {

        //first:="first.*"
        //next:="next"
 
	url := os.Getenv("NATS_URL")
	if url == "" {url = nats.DefaultURL}

	nc, _ := nats.Connect(url)
	defer nc.Drain()

	// start pipe service
	sub, _ := nc.Subscribe(first, func(msg *nats.Msg) {
		 //name := msg.Subject[6:]
                 //rep, _ := nc.Request("next.joe", []byte(name), time.Second)
                 rep, _ := nc.Request(next, []byte(msg.Subject), time.Second)

	         fmt.Println(string(rep.Data))
                 msg.Respond([]byte(string(f(nc,logsubj,rep.Data))))
                
	})

       log.Println("sub=",sub)  

	// ask service
	//rep, _ := nc.Request("first.joe", nil, time.Second)
	//fmt.Println(string(rep.Data))
	// stop service
	//sub.Unsubscribe()
	//
	//_, err := nc.Request("first.joe", nil, time.Second)
	//fmt.Println(err)
}

// pipe function

func f(nc *nats.Conn,logsubj string,inp []byte) []byte {
 nc.Publish(logsubj,inp)
 log.Printf("%v\n",string(inp)) 
 return inp
}

