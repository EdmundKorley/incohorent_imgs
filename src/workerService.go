package main

import (
    "os"
    "fmt"
    "net/http"
    "encoding/json"
    "time"
    "strconv"
    "image"
    "image/png"
    "image/color"
    "bytes"
    "sync"
    "io/ioutil"
)

type Task struct {
    ID int `json:"id"`
    State int `json:"state"`
}

var masterLocation string
var storageLocation string
var kVStoreAddress string

func main()  {
    if len(os.Args) < 3 {
        fmt.Println("Error 🚫: Too few arguments.")
        return
    }
    kVStoreAddress = os.Args[1]

    // Retreive master address
    response, err := http.Get("http://" + kVStoreAddress + "/get?key=masterAddress")
    if response.StatusCode != http.StatusOK {
        fmt.Println("Error 🚫: Can't get master address.")
        fmt.Println(response.Body)
    }
    data, err := ioutil.ReadAll(response.Body)
    if err != nil {
        fmt.Println(err)
        return
    }
    masterLocation = string(data)
    if len(masterLocation) == 0 {
        fmt.Println("Error 🚫: Can't get master address. Length is zero.")
        return
    }

    // Retreive the key-value store address
    response, err = http.Get("http://" + kVStoreAddress + "/get?key=storageAddress")
    if response.StatusCode != http.StatusOK {
        fmt.Println("Error: can't get storage address.")
        fmt.Println(response.Body)
        return
    }
    data, err = ioutil.ReadAll(response.Body)
    if err != nil {
        fmt.Println(err)
        return
    }
    storageLocation = string(data)
    if len(storageLocation) == 0 {
        fmt.Println("Error 🚫: can't get storage address. Length is zero.")
        return
    }

    // CL arg to set the number of concurrent threads
    threadCount, err := strconv.Atoi(os.Args[2])
    if err != nil {
        fmt.Println("Error 🚫: Couldn't parse thread count from command line arg")
        return
    }

    fmt.Println("workerService is up! 🔨")

    // Waiting for goroutines, as to don't terminate execution
    myWG := sync.WaitGroup{}
    myWG.Add(threadCount)
    for i := 0; i < threadCount; i++ {
        go func() {
            for {
                myTask, err := getNewTask(masterLocation)
                if err != nil {
                    fmt.Println(err)
                    fmt.Println("Waiting 2 second timeout...")
                    time.Sleep(time.Second * 2)
                    continue
                }

                myImage, err := getImageFromStorage(storageLocation, myTask)
                if err != nil {
                    fmt.Println(err)
                    fmt.Println("Waiting 2 second timeout...")
                    time.Sleep(time.Second * 2)
                    continue
                }

                myImage = doWorkOnImage(myImage)

                err = sendImageToStorage(storageLocation, myTask, myImage)
                if err != nil {
                    fmt.Println(err)
                    fmt.Println("Waiting 2 second timeout...")
                    time.Sleep(time.Second * 2)
                    continue
                }

                err = registerFinishedTask(masterLocation, myTask)
                if err != nil {
                    fmt.Println(err)
                    fmt.Println("Waiting 2 second timeout...")
                    time.Sleep(time.Second * 2)
                    continue
                }
            }
        }()
    }
}

// We make the request to the master and check if it was successful. We read the response body to
// memory and finally Unmarshal the response body to our Task structure. Finally we return it.
func getNewTask(masterAddress string) (Task, error) {
    response, err := http.Post("http://" + masterAddress + "/getNewTask", "text/plain", nil)
    if err != nil || response.StatusCode != http.StatusOK {
        return Task{-1, -1}, err
    }
    data, err := ioutil.ReadAll(response.Body)
    if err != nil {
        return Task{-1, -1}, err
    }

    myTask := Task{}
    err = json.Unmarshal(data, &myTask)
    if err != nil {
        return Task{-1, -1}, err
    }

    return myTask, nil
}

// We get the response whose body is the raw image, so we just Decode it and return it if we succeed.
func getImageFromStorage(storageAddress string, myTask Task) (image.Image, error) {
    response, err := http.Get("http://" + storageAddress + "/getImage?state=working&id=" + strconv.Itoa(myTask.ID))
    if err != nil || response.StatusCode != http.StatusOK {
        return nil, err
    }

    myImage, err := png.Decode(response.Body)
    if err != nil {
        return nil, err
    }

    return myImage, nil
}

// First we create a RGBA. That’s something like a canvas for drawing, and we create it with the size of our image. Later we draw on the canvas swapping the red with the green channel. Later we use the RGBA to return a new modified image, created from our canvas with the size of our original image.
func doWorkOnImage(myImage image.Image) image.Image {
    myCanvas := image.NewRGBA(myImage.Bounds())

    for i := 0; i < myCanvas.Rect.Max.X; i++ {
        for j := 0; j < myCanvas.Rect.Max.Y; j++ {
            r, g, b, _ := myImage.At(i, j).RGBA()
            myColor := new(color.RGBA)
            myColor.R = uint8(g)
            myColor.G = uint8(r)
            myColor.B = uint8(b)
            myColor.A = uint8(255)
            myCanvas.Set(i, j, myColor)
        }
    }

    return myCanvas.SubImage(myImage.Bounds())
}

// We create a data byte slice, and from that a data buffer which allows us to use it as a readwriter interface. We then use this interface to encode our image to png into, and finally send it using a POST to the server. If everything works out, then we just return.
func sendImageToStorage(storageAddress string, myTask Task, myImage image.Image) error {
    data := []byte{}
    buffer := bytes.NewBuffer(data)
    err := png.Encode(buffer, myImage)
    if err != nil {
        return err
    }
    response, err := http.Post("http://" + storageAddress + "/sendImage?state=finished&id=" + strconv.Itoa(myTask.ID), "image/png", buffer)
    if err != nil || response.StatusCode != http.StatusOK {
        return err
    }

    return nil
}

// We're done with processing image
func registerFinishedTask(masterAddress string, myTask Task) error {
    response, err := http.Post("http://" + masterAddress + "/registerTaskFinished?id=" + strconv.Itoa(myTask.ID), "test/plain", nil)
    if err != nil || response.StatusCode != http.StatusOK {
        return err
    }

    return nil
}
