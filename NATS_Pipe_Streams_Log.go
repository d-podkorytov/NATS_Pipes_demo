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

   for i:=2;i<len(os.Args)-2;i++ { fmt.Printf(" go pipe %v -> %v %v \n",os.Args[i],os.Args[len(os.Args)-2],os.Args[len(os.Args)-1])
                                   go pipe(os.Args[i],os.Args[len(os.Args)-2],os.Args[len(os.Args)-1])
                                 }
    
   // last pipe is in waiting state
   fmt.Printf(" pipe %v -> %v %v \n",os.Args[1],os.Args[len(os.Args)-2],os.Args[len(os.Args)-1])
   // pipe (from to log)
   pipe(os.Args[1],os.Args[len(os.Args)-2],os.Args[len(os.Args)-1])

}

func pipe(first string, next string, tolog string) {
 
	url := os.Getenv("NATS_URL")
	if url == "" {url = nats.DefaultURL}

	nc, err_nc := nats.Connect(url)
        if err_nc !=nil {log.Printf("%v",err_nc)
                         os.Exit(1) 
                        }
	defer nc.Drain()

	// start pipe service
	sub, err_sub := nc.Subscribe(first, func(msg *nats.Msg) {
                 		 //name := msg.Subject[6:]
                 //rep, _ := nc.Request("next.joe", []byte(name), time.Second)
                 rep, err_req := nc.Request(next, []byte(msg.Subject), time.Second)
                 if err_req !=nil {log.Printf("%v",err_req)
                                   os.Exit(1) 
                                  }
	         fmt.Println(string(rep.Data))
                 msg.Respond([]byte(string(f(nc,tolog,rep.Data))))
                
	})

       if err_sub !=nil {log.Printf("%v",err_sub)
                                      os.Exit(1) 
                                     }

       log.Println("sub=",sub," first=",first," next=",next," log=",tolog)  

	// ask service
	//rep, _ := nc.Request("first.joe", nil, time.Second)
	//fmt.Println(string(rep.Data))
	// stop servicing
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

