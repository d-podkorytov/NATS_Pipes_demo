package main


import (
	"fmt"
	"os"
	"time"
        "log" 
        "expvar"
        "encoding/binary"
	"encoding/json"

	"github.com/nats-io/nats.go"
)

var (
        Uptime         = expvar.NewInt("Uptime")  
        Errors         = expvar.NewInt("Errors")  

	Counter_Calls  = expvar.NewInt("Counter_Calls")
	Input_Bytes    = expvar.NewInt("Input_Bytes")
	Output_Bytes   = expvar.NewInt("Output_Bytes")

	// Momental speed at counters
	Speed_Counter_Calls  = expvar.NewInt("Speed_Counter_Calls")
	Speed_Input_Bytes    = expvar.NewInt("Speed_Input_Bytes")
	Speed_Output_Bytes   = expvar.NewInt("Speed_Output_Bytes")
	Speed_Errors         = expvar.NewInt("Speed_Errors")

        Metrics_Subj         = "metrics"
        Logger_Subj          = "logger"

        NATSConnection       *nats.Conn
        Metrics              serviceMetrics
)

type serviceMetrics struct {
	Id  string `json:"id"`

        Uptime  int64 `json:"Uptime"`
  
	Counter_Calls  int64  `json:"Counter_Calls"`
	Input_Bytes     int64 `json:"Input_Bytes"`
	Output_Bytes    int64 `json:"Output_Bytes"`
        Errors  int64         `json:"Errors"`  

	// Momental speed at counters
	Speed_Counter_Calls   int64 `json:"Speed_Counter_Calls"`
	Speed_Input_Bytes     int64 `json:"Speed_Input_Bytes"`
	Speed_Output_Bytes    int64 `json:"Speed_Output_Bytes"`
	Speed_Errors          int64 `json:"Speed_Errors"`

        Metrics_Subj         string `json:"Metrics"`
        Logger_Subj          string `json:"Logger"`

        //NATSConnection       *nats.Conn

}


func ZeroSpeedCounters() {
        // udate Metrics struct
        
        Metrics.Uptime         = Uptime.Value()  
        Metrics.Errors         = Errors.Value()  

	Metrics.Counter_Calls  = Counter_Calls.Value()
	Metrics.Input_Bytes    = Input_Bytes.Value()
	Metrics.Output_Bytes   = Output_Bytes.Value()

	// Momental speed at counters
	Metrics.Speed_Counter_Calls  = Speed_Counter_Calls.Value()
	Metrics.Speed_Input_Bytes    = Speed_Input_Bytes.Value()
	Metrics.Speed_Output_Bytes   = Speed_Output_Bytes.Value()
	Metrics.Speed_Errors         = Speed_Errors.Value()

        // probably it unsafe do it only once at first filling

        Metrics_Subj = os.Args[len(os.Args)-1]
        Logger_Subj  = os.Args[len(os.Args)-2]

        //Metrics.NATSConnection       *nats.Conn
        //Metrics.Metrics              serviceMetrics

        payload, err_payload := json.Marshal(Metrics)

        if err_payload==nil {NATSConnection.Publish(Metrics_Subj,[]byte(fmt.Sprintf("%v",payload)))}
        if err_payload!=nil {NATSConnection.Publish(Logger_Subj ,[]byte(fmt.Sprintf("%v",err_payload)))}
         
	Speed_Counter_Calls.Set(0)
	Speed_Input_Bytes.Set(0)
	Speed_Output_Bytes.Set(0)
        Speed_Errors.Set(0)
}

func main() {

      if len(os.Args) < 4 {fmt.Printf("Run this NATS pipe like \n%v input_subj1 ... input_subj_n  output_subj log_subj metrics_subj \n",os.Args[0])
                           os.Exit(1)
                          }

// Init heartbeat timer

    Metrics_Subj=os.Args[len(os.Args)-1]
    ticker := time.NewTicker(1000 * time.Millisecond)

    go func() {
        for {
            select {
             
             case t := <-ticker.C: {Uptime.Add(1) 
                                    ZeroSpeedCounters()
                                    //fmt.Println("\nvars=%#v\n",expvar) use expvar.Do for Dump all variables  
                                    fmt.Println("\nTick at", t," Uptime(sec)=",Uptime.Value(),"AVG Calls Speed=",Counter_Calls.Value()/Uptime.Value())
                                   }
            }
        }
    }()

   time.Sleep(10*time.Second)

 if len(os.Args) > 4 {
   for i:=3;i<len(os.Args)-3;i++ { fmt.Printf(" go pipe %v -> %v log=%v met=%v\n",os.Args[i],os.Args[len(os.Args)-3],os.Args[len(os.Args)-2],Metrics_Subj )
                                   go pipe(os.Args[i],os.Args[len(os.Args)-3],os.Args[len(os.Args)-2])
                                 }
   } 
   // last pipe is in waiting state
   fmt.Printf(" pipe %v -> %v log=%v met=%v\n",os.Args[1],os.Args[len(os.Args)-3],os.Args[len(os.Args)-2],Metrics_Subj)
   // pipe (from, to, log)
   pipe(os.Args[1],os.Args[len(os.Args)-3],os.Args[len(os.Args)-2])

}

func pipe(first string, next string, tolog string) {
        Counter_Calls.Add(1) 
  Speed_Counter_Calls.Add(1)

	url := os.Getenv("NATS_URL")
	if url == "" {url = nats.DefaultURL}

	nc, err_nc := nats.Connect(url)
        if err_nc !=nil {log.Printf("%v",err_nc)
                         Errors.Add(1) 
                   Speed_Errors.Add(1)

                         nc.Publish(tolog,[]byte(fmt.Sprintf("err_connect for %v %v",first,err_nc)))
                         os.Exit(1) 
                        }
	defer nc.Drain()
        NATSConnection=nc

	// start pipe service
 try_subscribing:=3

 subsribing:
        {try_subscribing--
	sub, err_sub := nc.Subscribe(first, func(msg *nats.Msg) {
                 		 //name := msg.Subject[6:]
                 //rep, _ := nc.Request("next.joe", []byte(name), time.Second)
                 rep, err_req := nc.Request(next, []byte(msg.Subject), time.Second)
                 if err_req !=nil {log.Printf("%v",err_req)
                                   Errors.Add(1) 
                             Speed_Errors.Add(1)
                                   nc.Publish(tolog,[]byte(fmt.Sprintf("error_request for %v %v",next,err_req)))
                                   //os.Exit(1) 
                                  }
	         fmt.Println(string(rep.Data))
                 msg.Respond([]byte(string(f(nc,tolog,rep.Data))))
                
	})

       if err_sub !=nil {log.Printf("%v",err_sub)
                         Errors.Add(1)
                   Speed_Errors.Add(1)
                         nc.Publish(tolog,[]byte(fmt.Sprintf("error_subscribe for %v %v",first,err_sub)))             
                         //os.Exit(1) 
                        }
       log.Println("sub=",sub," first=",first," next=",next," log=",tolog," metrics=",Metrics_Subj )  

       }
       if try_subscribing > 0 {goto subsribing}
  
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

 Input_Bytes.Add(int64(len(inp)))
 Speed_Input_Bytes.Add(int64(len(inp)))

 // echo inp to log
 nc.Publish(logsubj,inp)
 // watch out lost presion binary.Size() is returning int
 log.Printf("%v\n",string(inp)) 
 //

 Output_Bytes.Add(int64(binary.Size(inp)))
 Speed_Output_Bytes.Add(int64(len(inp)))

 return inp
}

