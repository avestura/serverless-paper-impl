namespace Avestura.Serverless

open System

type ServerlessFunction = {
    name: string
    deps: string list
}

type QueueFunctionRequest = {
    func: ServerlessFunction
    startTime: DateTime
    serviceTime: TimeOnly
    packageRestorationTime: TimeOnly
}