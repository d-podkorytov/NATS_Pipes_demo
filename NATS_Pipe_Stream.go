package main


import (
	"fmt"
	"os"
	"time"
        "log" 

	"github.com/nats-io/nats.go"
)

func main() {
   //fmt.Println("Args = %#v\n",os.Args)
   if len(os.Args) < 3 {fmt.Printf("Run this NATS pipe like \n%v input_subj output_subj\n",os.Args[0])
                        os.Exit(1)
                       }

//   pipe("first.*","next")
   pipe(os.Args[1]+".*",os.Args[2])

}

func pipe(first string, next string) {

	url := os.Getenv("NATS_URL")
	if url == "" {url = nats.DefaultURL}

	nc, nc_err := nats.Connect(url)

        if nc_err!=nil {log.Printf("connection error: %v\n",nc_err)
                        os.Exit(1)
                       }

	defer nc.Drain()

	// start pipe service
	sub, sub_err := nc.Subscribe(first, func(msg *nats.Msg) {
		 //name := msg.Subject[5:]
                 //rep, _ := nc.Request("next.alice", []byte(name), time.Second)
                 rep, rep_err := nc.Request(next, []byte(msg.Subject), time.Second)
                 if rep_err!=nil {log.Printf("repling error: %v\n",rep_err)}

	         if rep_err==nil {fmt.Println(string(rep.Data))
                                  msg.Respond([]byte(string(f(rep.Data))))
                 }
                
	})


                 if sub_err!=nil {log.Printf("subscribe error: %v\n",sub_err)}

	         if sub_err==nil {log.Println("sub=",sub)}  

	// ask service
	//rep, _ := nc.Request("first.alice", nil, time.Second)
	//fmt.Println(string(rep.Data))
	// stop service
	//sub.Unsubscribe()
	//
	//_, err := nc.Request("first.alice", nil, time.Second)
	//fmt.Println(err)
}

// pipe function example for handling data stream
func f(inp []byte) []byte {
 log.Printf("%v\n",string(inp)) 
 return inp
}

