// Run function call(string)-> string for of GoMacro script for every incoming message from n data sources
// log errors to logging_subject
// pass metrics to metrics_subject
// DEBUG: read code from file
//        read code from nc.SubscribeSync
// TODO:  read code updates from nc.Subscribe Async
//        refactor  pipe function for moving nc connection outside 
//        clean code, move to shared state logger and metrics subjects

package main

import (
	"fmt"
	"os"
	"time"
        "log" 
        "expvar"
        "encoding/binary"
	"encoding/json"
//        "reflect"
        "bufio"

        "github.com/cosmos72/gomacro/fast"
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

        // for sharing , logging and management 
        Metrics_Subj         = expvar.NewString("metrics")
        Logger_Subj          = expvar.NewString("logger")
        Code_Subject         = expvar.NewString("code_subj")   

        NATSConnection       *nats.Conn
        Metrics              serviceMetrics

        Call_Code            = expvar.NewString("\nfunc call(inp string) string {\nreturn inp}\n")
  // for Reduce it'll be "\nfunc call(inp string) string {\nreturn Shared_Result=Shared_Result+inp}\n" and publish Shared_Result to given next_subj inside ticker timer

        Shared_Result        = expvar.NewString("") // Shared State for Fold and Reduce

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

        Metrics_Subj.Set(os.Args[len(os.Args)-1])
        Logger_Subj.Set(os.Args[len(os.Args)-2])

        //Metrics.NATSConnection       *nats.Conn
        //Metrics.Metrics              serviceMetrics

        // publish metrics struct
        payload, err_payload := json.Marshal(Metrics)

        if err_payload==nil {NATSConnection.Publish(Metrics_Subj.Value(),[]byte(fmt.Sprintf("code here","%v",payload)))}
        if err_payload!=nil {NATSConnection.Publish(Logger_Subj.Value(),[]byte(fmt.Sprintf("%v",err_payload)))}
         
	Speed_Counter_Calls.Set(0)
	Speed_Input_Bytes.Set(0)
	Speed_Output_Bytes.Set(0)
        Speed_Errors.Set(0)
}

func main() {

      if len(os.Args) < 4 {fmt.Printf("Run this NATS pipe like \n%v input_subj1 ... input_subj_n  output_subj log_subj metrics_subj \n",os.Args[0])
                           os.Exit(1)
                          }

// open NATS
	url := os.Getenv("NATS_URL")

        Logger_Subj.Set(os.Args[len(os.Args)-2])
        Metrics_Subj.Set(os.Args[len(os.Args)-1])

	if url == "" {url = nats.DefaultURL}

	nc, err_nc := nats.Connect(url)
        if err_nc !=nil {log.Printf("%v",err_nc)
                         Errors.Add(1) 
                   Speed_Errors.Add(1)

                         nc.Publish(Logger_Subj.Value(),[]byte(fmt.Sprintf("err_nats_connect  %v",err_nc)))
                         os.Exit(1) 
                        }

	defer nc.Drain()

        NATSConnection=nc

// Init heartbeat timer

    ticker := time.NewTicker(1000 * time.Millisecond)
    Code_Subject.Set("code_subject")

// get Call_Code from file or Subscribe next

// File_Name:="f"
// code_bytes,err_code:= ReadFile(File_Name)
/*
  code_bytes,err_code:= ReadSubject(nc,Code_Subject)

 if err_code != nil { log.Printf("err_code %v %v",File_Name,err_code)
                      os.Exit(1)
                    }

 if err_code == nil {Call_Code.Set(string(code_bytes))}
*/
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
                                   go pipe(nc,os.Args[i],os.Args[len(os.Args)-3],os.Args[len(os.Args)-2])
                                 }
   } 
   // last pipe is in waiting state
   fmt.Printf(" pipe %v -> %v log=%v met=%v\n",os.Args[1],os.Args[len(os.Args)-3],os.Args[len(os.Args)-2],Metrics_Subj)
   // pipe (from, to, log)
   pipe(nc,os.Args[1],os.Args[len(os.Args)-3],os.Args[len(os.Args)-2])

}

func pipe(nc *nats.Conn,first string, next string, tolog string) {
        Counter_Calls.Add(1) 
  Speed_Counter_Calls.Add(1)
/*
	url := os.Getenv("NATS_URL")
	if url == "" {url = nats.DefaultURL}

	nc, err_nc := nats.Connect(url)
        if err_nc !=nil {log.Printf("%v",err_nc)
                         Errors.Add(1) 
                   Speed_Errors.Add(1)

                         nc.Publish(Logger_Subj.Value(),[]byte(fmt.Sprintf("err_connect for %v %v",first,err_nc)))
                         os.Exit(1) 
                        }

	defer nc.Drain()

        NATSConnection=nc
*/
// Read code from Subscription
  code_bytes,err_code:= ReadSubject(nc,Code_Subject.Value())

 if err_code != nil { log.Printf("err_code %v %v",Code_Subject,err_code)
                      os.Exit(1)}

 if err_code == nil {Call_Code.Set(string(code_bytes))}

//  code_bytes,err_code:= ReadSubject(nc,Code_Subject)

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
                                   nc.Publish(Logger_Subj.Value(),[]byte(fmt.Sprintf("error_request for %v %v",next,err_req)))
                                   //os.Exit(1) 
                                  }
	         fmt.Println(string(rep.Data))
                 msg.Respond([]byte(string(f(Call_Code.Value(),nc,Logger_Subj.Value(),rep.Data))))
                
	})

       if err_sub !=nil {log.Printf("%v",err_sub)
                         Errors.Add(1)
                   Speed_Errors.Add(1)
                         nc.Publish(Logger_Subj.Value(),[]byte(fmt.Sprintf("error_subscribe for %v %v",first,err_sub)))             
                         //os.Exit(1) 
                        }
       log.Println("sub=",sub," first=",first," next=",next," log=",Logger_Subj.Value()," metrics=",Metrics_Subj )  

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

func f(call_code string,nc *nats.Conn,logsubj string,inp []byte) []byte {

 Input_Bytes.Add(int64(len(inp)))
 Speed_Input_Bytes.Add(int64(len(inp)))
// out:=[]byte(RunGomacro(call_code+"\n func call(inp string) string {\nreturn inp}\n",string(inp)))
// call_code like " func call(inp string) string {\nreturn inp}\n"
 out:=[]byte(RunGomacro(nc,call_code,string(inp)))

 // echo out to log
 nc.Publish(logsubj,out)
 // watch out lost presion binary.Size() is returning int
 log.Printf("%v\n",string(out)) 
 //

 Output_Bytes.Add(int64(binary.Size(out)))
 Speed_Output_Bytes.Add(int64(len(out)))

 return out
}

//func RunGomacro(toeval string) reflect.Value {
func RunGomacro(nc *nats.Conn,call string,toeval string) string {
    interp  := fast.New()
    // define fun call(string) string in file
    vals, err_vals := interp.Eval(call+"\n"+"call("+toeval+")\n")
    // for simplicity, only use the first returned value
    if err_vals==nil {return fmt.Sprintf("%v",vals[0].ReflectValue())}

    Errors.Add(1)
    return fmt.Sprintf("error %v %v",toeval, err_vals)
}

func ReadFile(filename string) ([]byte, error) {
    file, err := os.Open(filename)

    if err != nil { return nil, err}
    defer file.Close()

    stats, statsErr := file.Stat()
    if statsErr != nil { return nil, statsErr}

    var size int64 = stats.Size()
    bytes := make([]byte, size)

    bufr := bufio.NewReader(file)
    _,err = bufr.Read(bytes)

    return bytes, err
}


func ReadSubject(nc *nats.Conn,code_subject string) ([]byte, error) {

 	sub, sub_err := nc.SubscribeSync(code_subject) //+".*")

        if sub_err!=nil {return []byte(fmt.Sprintf("error subsribing %v %#v",code_subject,sub)), sub_err }

	msg, msg_err := sub.NextMsg(10 * time.Millisecond)

        return msg.Data, msg_err
}

func ReadSubjectAsync(nc *nats.Conn,code_subject string) bool {

        if NATSConnection == nil { log.Printf(" NATSConnection == nil at code subsription ")
                                   return false 
                                 }

 	_, sub_err := NATSConnection.Subscribe(code_subject, func(msg *nats.Msg) { if msg.Data != nil {Call_Code.Set(fmt.Sprintf("%v",msg.Data))} })

        if sub_err!=nil {//return []byte(fmt.Sprintf("error subsribing %v %#v",code_subject,sub)), sub_err 
                          return false 
                        }

        return true
}