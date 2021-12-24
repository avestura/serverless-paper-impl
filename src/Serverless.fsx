namespace Avestura.Serverless

open NodaTime
open System.Text.Json
open System.Text.Json.Serialization
open System.Collections.Generic
open MathNet.Numerics.Distributions

#r "nuget: FSharp.Data"
#r "nuget: NodaTime"
#r "nuget: MathNet.Numerics"
#r "nuget: MathNet.Numerics.FSharp"

open System
open System.IO
open System.Text.Json
open FSharp.Data
open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames
open System.Text.Json
open System.Text.Json.Serialization

[<Measure>] type dbyte
[<Measure>] type kilobyte
[<Measure>] type megabyte

module General =
    open MathNet.Numerics.Distributions
    let rnd = Random()

    let NumberOfFunctionDeps_DefaultNormalDistMean = 50
    let NumberOfFunctionDeps_DefaultNormalDistStdDev = 15

    let FunctionDuration_MaxRuntimeDuration = 300<second>
    let FunctionDuration_DefaultChiSquareFreedom = 2.1
    let FunctionDuration_MinRuntimeDuration = 1.0<second>

    let FunctionRunRequestEvery_NormalMean = 30.0<second>
    let FunctionRunRequestEvery_NormalStdDev = 10.0<second>
    
    let FunctionSimilarity_TopNCommonDependencies = 10

    let PackageRestoreSpeed = 2.<second/megabyte>


    let inline weilbullStretched x a b = 1. - (Math.E ** (-1. * ((x/a) ** b)))
    let inline normalize x = min 1. (weilbullStretched x 5. 2.)

    let bytesToKilobytes (x: float<dbyte>) = x / (1000.0<dbyte/kilobyte>)
    let kilobytesToMegabytes (x: float<kilobyte>) = x / (1000.0<kilobyte/megabyte>)
    let bytesToMegabytes = bytesToKilobytes >> kilobytesToMegabytes

    let calculateRestorationTime (size: float<dbyte>) =
        let mb = bytesToMegabytes size
        let sec = PackageRestoreSpeed * mb

        let strippedSec = float sec 

        Sample.normal strippedSec (strippedSec / 5.) rnd |> abs |> ceil |> int64 |> (*) 1L<second>

    let calculateRestorationDuration (size: float<dbyte>) =
        size |> calculateRestorationTime |> int64 |> Duration.FromSeconds
 
module NodaTimeUtils =
    let addDuration (d: Duration) (lt: LocalTime) =
        lt.PlusTicks(d.TotalTicks |> int64)

module DSExtensions =
    open General

    let pickRandomItemFromList (list: 'a list) =
        list.[rnd.Next(0, list.Length)]

    let makeProbMap (m: Map<string, int>) =
        let sum = m.Values |> Seq.sum |> float
        m |> Map.map (fun key value -> (float value) / sum)

    let makeProbMapFloat (m: Map<string, float>) =
        let sum = m.Values |> Seq.sum
        m |> Map.map (fun key value -> (float value) / sum)

    let russianRouletteMap inputMap =
        let mutable prevValue = 0.0
        inputMap |> Map.map (fun key value -> 
            let newValue = value + prevValue
            prevValue <- newValue
            newValue
        )

    let takeNFromProbMap n inputMap =
        let rec takeNFromRussianRouletteMapRec (inputMap: Map<string, float>) n result =
            
            match n with
            | _ when n <=0 -> result
            | n -> 
                let random = rnd.NextDouble()
                // printfn "random: %f" random
                let rr = russianRouletteMap inputMap
                //printfn "rr map: %A" rr
                let candidKey = 
                    rr |> Map.pick (fun k v -> if random <= v then Some k else None)
                // Console.WriteLine $"{candidKey} was removed with prob {candidProbability}"
                
                let purgedMap = inputMap |> Map.remove candidKey
                let newMap = makeProbMapFloat purgedMap
                // Console.WriteLine $"per item incr: {perItemIncr}"
                takeNFromRussianRouletteMapRec newMap (n - 1) (result@[candidKey])

        let mapSize = Map.count inputMap
        let cleansedMap = inputMap |> Map.filter (fun k v -> v <> 0.0)
        let cleansedMapSize = Map.count cleansedMap
        let removedZerosCount = mapSize - cleansedMapSize
        
        if n > (mapSize - removedZerosCount) then failwith $"n={n} can't be biger than mapSize-zeroProbs({mapSize}-{removedZerosCount})"

        takeNFromRussianRouletteMapRec cleansedMap n []

    let rec insertSorted item = function
    | x::xs -> min x item::(insertSorted (max x item) xs)
    | _ -> [item]

    let listSubtract (a: 'a list) (b: 'a list) =
        let unique = set b
        a |> List.filter (fun x -> unique.Contains x = false)

module PaperDataUtils = 
    let convertJsonValue (converter: JsonValue -> 'a) (props: ('b * JsonValue) array) =
        props |> Array.map(fun (key, value) -> (key, converter value)) |> Map.ofArray

    let convertKey (converter: string -> 'a) (props: (string * JsonValue) array) =
        props |> Array.map(fun (key, value) -> (converter key, value))

type PackageSizeInfo = {
    name: string
    install: {|
        bytes: int
        pretty: string
    |}
    publish: {|
        bytes: int
        pretty: string
    |}
}

type PackageFullInfo = {
    name: string
    dependencyCount: int
    install: {|
        bytes: int
        pretty: string
    |}
    publish: {|
        bytes: int
        pretty: string
    |}
}

module PackagesData = 
    open DSExtensions
    open PaperDataUtils
    type DependentUponDataType = JsonProvider<"../data/packageData/dependentUpon.json">
    type HitsRankDataType = JsonProvider<"../data/packageData/hitsrank.json">
    type PageRankDataType = JsonProvider<"../data/packageData/pagerank.json">
    type PackagePhobiaResponse = JsonProvider<"../data/schemas/packagephobia_repsonse.json">
    type DependentUponSizeData = JsonProvider<"../data/packageData/size_partial_data/dependentUpon_sizes_part1.json">

    let packageNames =
        DependentUponDataType.GetSample().JsonValue.Properties() |> Array.map(fun (x, y) -> x)

    let dependentUpon = 
        DependentUponDataType.GetSample().JsonValue.Properties()  |> convertJsonValue (fun x -> x.AsInteger())
       
    let fullAllPackagesData =
        let json = File.ReadAllText("../data/packageData/dependentUpon_full.json")
        JsonSerializer.Deserialize<Dictionary<string, PackageFullInfo>>(json)

    let getFullPackageDataSafe packageName =
        let holes = [
            "xml2js"
            "cookie-parser"
            "fsevents"
            "highlight.js"
            "puppeteer"
            "serialport"
            "pump"
            "url-parse"
            "requirejs"
            "text-table"
            "fs-promise"
            "yargs-parser"
            "phantomjs-prebuilt"
            "phantomjs"
            "hiredis"
            "serialize-javascript"
            "imagemin-pngquant"
            "xml2json"
            "yeoman-environment"
            "karma-mocha"
            "karma-phantomjs-launcher"
        ]

        if holes |> List.contains packageName then
            {
                name = packageName
                dependencyCount = fullAllPackagesData.[packageName].dependencyCount
                install = {|
                    bytes = 3841636 // Average of other packages with install data
                    pretty = "<mean>"
                |}
                publish = {|
                    bytes = 1696329 // Average of other packages with publish data
                    pretty = "<mean>"
                |}
            }
        else
            fullAllPackagesData.[packageName]

    let fullAllPackagesDataSafe = 
        fullAllPackagesData.Keys
            |> List.ofSeq
            |> List.map (fun x -> x, getFullPackageDataSafe x)
            |> Map.ofList
        
    let probOfDependency packageName =
        let sum = dependentUpon.Values |> Seq.sum |> float
        (float dependentUpon.[packageName]) / sum

    let probOfDependencies = makeProbMap dependentUpon

    let hitsrank =
        HitsRankDataType.GetSample().JsonValue.Properties() |> convertJsonValue (fun x -> x.AsFloat())

    let pagerank =
        PageRankDataType.GetSample().JsonValue.Properties() |> convertJsonValue (fun x -> x.AsFloat())

    let fetchPackageSizeData packageName =
        let result = PackagePhobiaResponse.Load($"https://packagephobia.com/v2/api.json?p=%s{packageName}")

        {
            name = result.Name
            install = {|
                bytes = result.Install.Bytes
                pretty = result.Install.Pretty
            |}
            publish = {|
                bytes = result.Publish.Bytes
                pretty = result.Publish.Pretty
            |}
        }
    
    let downloadAndSaveAllPackageSizes (items: string list) =
        let rec download (remainingItems: string list) (result: Map<string, PackageSizeInfo>) =
            match remainingItems with
            | [] -> 
                File.WriteAllText("../data/packageData/dependentUpon_sizes.json", JsonSerializer.Serialize(result))
                printfn "Save completed."
                result
            | pkgName::otherPkgs ->
                printfn $"Download data for package {pkgName}"
                let sizeData = fetchPackageSizeData pkgName
                File.WriteAllText("../data/packageData/dependentUpon_sizes.json", JsonSerializer.Serialize(result))
                download otherPkgs (result |> Map.add pkgName sizeData)

        download items (Map.ofList [])

module FrequenciesData =
    open PaperDataUtils
    type FreqDataType = JsonProvider<"../data/frequencies/freqData.json">

    let frequencies = 
        FreqDataType.GetSample().JsonValue.Properties()
        |> convertJsonValue (fun x -> x.Properties() |> convertKey int |> convertJsonValue (fun y -> y.AsInteger()))

open NodaTime

type ServerlessFunction = {
    name: string
    deps: string list
}
with
    member this.getFullDependencySize() =
        this.deps
        |> List.map PackagesData.getFullPackageDataSafe 
        |> List.map (fun x -> x.install.bytes)
        |> List.sum
        |> (*) 1<dbyte>
    member this.getNonCachedDependencySize (cachedFunc: ServerlessFunction) =
        let notCachedDeps = DSExtensions.listSubtract this.deps cachedFunc.deps
        notCachedDeps
        |> List.map PackagesData.getFullPackageDataSafe 
        |> List.map (fun x -> x.install.bytes)
        |> List.sum
        |> (*) 1<dbyte>
    member this.fullRestorationDuration() = 
        this.getFullDependencySize()
        |> float
        |> (*) 1.0<dbyte>
        |> General.calculateRestorationDuration
    member this.nonCachedRestorationDuration (cachedFunc: ServerlessFunction) =
        this.getNonCachedDependencySize cachedFunc
        |> float
        |> (*) 1.0<dbyte>
        |> General.calculateRestorationDuration

module ServerlessFunctionUtils = 
    open PackagesData
    open System.Linq
    open General
    let similarity (f1: ServerlessFunction) (f2: ServerlessFunction) =
        if f1.name = f2.name then 1.
        else
            let orderedDeps (f: ServerlessFunction) =
                f.deps
                |> List.map getFullPackageDataSafe
                |> List.sortBy (fun x -> x.install.bytes)

            let od1 = orderedDeps f1
            let od2 = orderedDeps f2
            let intersect =
                (Seq.ofList od1).Intersect(od2)
                |> Seq.take FunctionSimilarity_TopNCommonDependencies
                |> Seq.sumBy (fun x -> x.install.bytes)

            normalize intersect



type EnvironmentContext = {
    currentDayOfWeek: string
    currentTime: LocalTime
}

module FunctionGenerateOptions =
    type DependenciesCount =
        | DataUnawareRandomUniform of n : int
        | DataUnawareRandomNormal of mean : int * stddev: int
        | DataAwareRandomUniform of data : Map<string,int> option * n : int
        | DataAwareRandomNormal of data : Map<string, int> option * n : int * stddev : int

module FunctionGenerator =
    open General
    open FunctionGenerateOptions
    open DSExtensions
    open MathNet.Numerics.Distributions
    let getNRandomPackageNames n = 
        let allNames = PackagesData.packageNames
        allNames |> Array.sortBy(fun _ -> rnd.Next()) |> Array.take n

    let generateFunctionData (deps: FunctionGenerateOptions.DependenciesCount) n =
        match deps with
        | DataUnawareRandomUniform c ->
            let nNames = getNRandomPackageNames c
            [1..n] |> List.map (fun i -> {
                name = $"f{i}"
                deps = nNames |> List.ofArray
            })
        | DataUnawareRandomNormal (m, s) ->
            let c = Normal.Sample(float m, float s) |> abs |> ceil |> int
            let names = getNRandomPackageNames c
            [1..n] |> List.map (fun i -> {
                name = $"f{i}"
                deps = names |> List.ofArray
            })
            
        | DataAwareRandomUniform (maybeData, c) ->
            let data = 
                match maybeData with
                | Some data -> data
                | None -> PackagesData.dependentUpon

            let probData = makeProbMap data

            [1..n] |> List.map (fun i -> {
                name = $"f{i}"
                deps = probData |> takeNFromProbMap c
            })

        | DataAwareRandomNormal (maybeData, m, s) ->
            let data = 
                match maybeData with
                | Some data -> data
                | None -> PackagesData.dependentUpon

            let probData = makeProbMap data
            let c = Normal.Sample(float m, float s) |> abs |> ceil |> int

            [1..n] |> List.map (fun i -> {
                name = $"f{i}"
                deps = probData |> takeNFromProbMap c
            })

module QueueFunctionGeneration = 
    open General
    open FunctionGenerateOptions
    type DayOfWeekName = string
    type FreqHour = int    

    type FunctionCoopMode =
        | IgnoreCoopNetwork
        | UseCoopNetwork

    type FrequencyOfInvocationMode = 
        | IgnoreFrequencyData
        | UseFrequencyData of Map<DayOfWeekName,Map<FreqHour,int>>

    let defaultUnawareRandomNormalDepsCount = DataUnawareRandomNormal (NumberOfFunctionDeps_DefaultNormalDistMean, NumberOfFunctionDeps_DefaultNormalDistStdDev)
    let defaultAwareRandomNormalDepsCount = DataAwareRandomNormal (None, NumberOfFunctionDeps_DefaultNormalDistMean, NumberOfFunctionDeps_DefaultNormalDistStdDev)
    type Options = FrequencyOfInvocationMode * FunctionCoopMode * FunctionGenerateOptions.DependenciesCount


type QueueFunctionRequest = {
    func: ServerlessFunction
    startTime: LocalTime
    serviceTime: Duration
}

module QueueDataGenerator =
    open General
    open FunctionGenerator
    open DSExtensions
    open MathNet.Numerics.Distributions
    open QueueFunctionGeneration
    let generateFunctionDuration() = 
        let chi = Sample.chiSquared 2.1 rnd |> (*) 30.0<second> 
        let min = FunctionDuration_MinRuntimeDuration
        if chi < min then min else chi

    let getNextFunctionRequestTimeDiff (option: FrequencyOfInvocationMode) (currentDayOfWeek: string) (currentTime: LocalTime) =
        match option with
        | IgnoreFrequencyData ->
            let m = FunctionRunRequestEvery_NormalMean |> (*) 1.0<1/second>
            let s = FunctionRunRequestEvery_NormalStdDev |> (*) 1.0<1/second>
            Sample.normal m s rnd |> abs |> ceil |> int64 |> (*) 1L<second>
        | UseFrequencyData data ->
            let normalMeanOf callFreqPerHour = 3600.0 / callFreqPerHour
            let stdDevOf normalMean = normalMean / 3.0
            let currentHour = currentTime.Hour
            let currentFreq = data.[currentDayOfWeek].[currentHour]
            let m = normalMeanOf currentFreq
            let s = stdDevOf m
            Sample.normal m s rnd |> abs |> ceil |> int64 |> (*) 1L<second>

    let generateFunctionQueueData (options: QueueFunctionGeneration.Options) (numberOfFunctions: int) = 
        let terminationCondition (prevTime: LocalTime) (newTime: LocalTime) =
            prevTime.Hour = 23 && newTime.Hour = 0

        let (freqMode, coopMode, depencencyMode) = options

        let funcList = generateFunctionData depencencyMode numberOfFunctions

        let rec generate (currentTime: LocalTime) (result: QueueFunctionRequest list) =
            let secsToAdd = getNextFunctionRequestTimeDiff freqMode "saturday" currentTime * (1L<1/second>)
            let startTime = currentTime.PlusSeconds(secsToAdd)
            let serviceTime = generateFunctionDuration() |> (*) 1.0<1/second>
            if terminationCondition currentTime startTime then result
            else
                let nextFunctionToPick = 
                    match coopMode with
                    | IgnoreCoopNetwork -> pickRandomItemFromList funcList
                    | UseCoopNetwork -> pickRandomItemFromList funcList // TODO: must be fixed

                let newItem = {
                    func = nextFunctionToPick
                    startTime = startTime
                    serviceTime = Duration.FromSeconds serviceTime
                }

                generate startTime (result@[newItem])
            
        generate (LocalTime(0,0,0)) []
        
type Container = {
    name: string
    runningFunction: ServerlessFunction option
    created: LocalTime
    finishedExecTime: LocalTime option
    disposed: LocalTime option
    totalWorkingDuration: Duration
    totalWaitForNextFunctionDuration: Duration
    totalRestorationDuration: Duration
    chanceofWatiningMore: float 
    previousFunctions: ServerlessFunction list
}
    with
        member this.isDisposed() = this.disposed.IsSome
        member this.numberOfTimesUsed() = this.previousFunctions |> List.length
        member this.isWaitingForNextFunction() = this.finishedExecTime.IsSome && (this.isDisposed() |> not)
        static member makeNew name func created =
            {
                name = name
                runningFunction = func
                created = created
                finishedExecTime = None
                disposed = None
                totalWorkingDuration = Duration.Zero
                totalWaitForNextFunctionDuration = Duration.Zero
                totalRestorationDuration = Duration.Zero
                chanceofWatiningMore = 0.0
                previousFunctions = []
            }
        member this.terminate disposeTime =
            {
                this with 
                    disposed = Some disposeTime
                    finishedExecTime = Some disposeTime
                    runningFunction = None
                    previousFunctions = match this.runningFunction with Some x -> this.previousFunctions@[x] | _ -> this.previousFunctions
            }
        member this.finishExec finishTime =
            { this with finishedExecTime = Some finishTime }

type TimelineEventKind =
    | QueueRequest of QueueFunctionRequest // A new function wants to run in the serverless platform
    | FinishRunningFunction of Container // A function finished its execution in container
    | MountWaitTimedOut of Container // Container waited some time epoch for another function to join but nothing happened

[<CustomComparison;CustomEquality>]
type TimelineEvent = {
    time: LocalTime
    kind: TimelineEventKind
} with
    override x.Equals(yobj) = 
        match yobj with
        | :? TimelineEvent as y -> (x.time = y.time)
        | _ -> false

    override x.GetHashCode() = hash (x.time)
    interface System.IComparable with
        member x.CompareTo yobj =
            match yobj with
            | :? TimelineEvent as y -> compare (x.time) (y.time)
            | _ -> invalidArg "yobj" "cannot compare value of different types"


type SimulatorContext = {
    day: string
    events: TimelineEvent list
    containerIdCounter: int
    containers: Map<string, Container>
}
with
    member this.getMountableContainers() =
        this.containers.Values
        |> List.ofSeq
        |> List.filter (fun x -> x.isWaitingForNextFunction())

type Scheduler = TimelineEvent -> SimulatorContext -> SimulatorContext

module Simulator =
    open DSExtensions
    let insertEvent (item: TimelineEvent) (evList: TimelineEvent list) =
        insertSorted item evList

    let rec runSimulation (scheduler: Scheduler) (context: SimulatorContext) =
        match context.events with
        | [] -> context
        | event::restEvents -> 
            let popEventContext = { context with events = restEvents }
            let newContext = scheduler event popEventContext
            runSimulation scheduler newContext

    let convertQueueRequestDataToSimulatorContext (day: string) (queueFuncReq: QueueFunctionRequest list) =
        {
            day = day
            events = queueFuncReq |> List.map (fun qfr -> { time = qfr.startTime ; kind = QueueRequest qfr })
            containerIdCounter = 0
            containers = Map.empty
        }

    let runSimulatonWithQueueData (day: string) (queueFuncData: QueueFunctionRequest list) (scheduler: Scheduler) =
        let ctx = convertQueueRequestDataToSimulatorContext day queueFuncData
        runSimulation scheduler ctx

module Schedulers =
    open General
    open Simulator
    let doNothingScheduler : Scheduler = fun _ ctx -> ctx
    let noMergeScheduler: Scheduler = fun ev ctx ->
        let (time, kind) = ev.time, ev.kind
        match kind with
        | QueueRequest qfr ->
            let restoreDuration = qfr.func.fullRestorationDuration()
            let functionFinish = time.PlusNanoseconds((restoreDuration + qfr.serviceTime).ToInt64Nanoseconds())
            let newContainer = {
                name = $"container-%d{ctx.containerIdCounter}"
                runningFunction = Some qfr.func
                created = time
                finishedExecTime = None
                disposed = None
                totalRestorationDuration = restoreDuration
                totalWaitForNextFunctionDuration = Duration.Zero
                totalWorkingDuration = qfr.serviceTime
                chanceofWatiningMore = 0.0
                previousFunctions = []
            }
            { ctx with
                containerIdCounter = ctx.containerIdCounter + 1
                containers = ctx.containers |> Map.add newContainer.name newContainer
                events = ctx.events |> insertEvent {
                    time = functionFinish
                    kind = FinishRunningFunction newContainer
                }
            }
        | FinishRunningFunction container -> 
            let terminatedContainer = container.terminate ev.time
            let newConts = ctx.containers |> Map.add container.name terminatedContainer
            { ctx with containers = newConts }
        | _ -> ctx

    let computedWaitScheduler (getWaitTime: unit -> int<second>): Scheduler = fun ev ctx ->
        let (time, kind) = ev.time, ev.kind
        match kind with
        | QueueRequest qfr ->
            let mountConts = ctx.getMountableContainers()
            match mountConts with
            | [] ->
                let restoreDuration = qfr.func.fullRestorationDuration()
                let functionFinish = time.PlusNanoseconds((restoreDuration + qfr.serviceTime).ToInt64Nanoseconds())
                let newContainer = {
                    name = $"container-%d{ctx.containerIdCounter}"
                    runningFunction = Some qfr.func
                    created = time
                    finishedExecTime = None
                    disposed = None
                    totalRestorationDuration = restoreDuration
                    totalWaitForNextFunctionDuration = Duration.Zero
                    totalWorkingDuration = qfr.serviceTime
                    chanceofWatiningMore = 0.0
                    previousFunctions = []
                }
                { ctx with
                    containerIdCounter = ctx.containerIdCounter + 1
                    containers = ctx.containers |> Map.add newContainer.name newContainer
                    events = ctx.events |> insertEvent {
                        time = functionFinish
                        kind = FinishRunningFunction newContainer
                    }
                }
            | conts ->
                let host = DSExtensions.pickRandomItemFromList conts
                let restorationTime = qfr.func.nonCachedRestorationDuration host.runningFunction.Value
                let waitTime = (time - host.finishedExecTime.Value).ToDuration()
                let finishTime = time.PlusNanoseconds((restorationTime + qfr.serviceTime).ToInt64Nanoseconds())
                let modifiedHost = {
                    host with
                        runningFunction = Some qfr.func
                        finishedExecTime = None
                        totalRestorationDuration = host.totalRestorationDuration + restorationTime
                        totalWaitForNextFunctionDuration = host.totalWaitForNextFunctionDuration + waitTime
                        totalWorkingDuration = host.totalWorkingDuration + qfr.serviceTime
                        previousFunctions = host.previousFunctions @ [host.runningFunction.Value]
                }
                {
                    ctx with
                        containers = ctx.containers |> Map.add modifiedHost.name modifiedHost
                        events = ctx.events |> insertEvent {
                            time = finishTime
                            kind = FinishRunningFunction modifiedHost
                        }
                }


        | FinishRunningFunction cont ->
            let newCont = {
                cont with
                    finishedExecTime = Some time
            }
            { ctx with
                containers = ctx.containers |> Map.add newCont.name newCont      
            }
        | MountWaitTimedOut cont ->
            ctx

    let staticWaitScheduler (waitTime: int<second>): Scheduler = computedWaitScheduler (fun () -> waitTime)
    let randomWaitScheduler (mean: int<second>) (stddev: int<second>): Scheduler =
        computedWaitScheduler (fun () -> Normal.Sample (float mean, float stddev) |> ceil |> int |> (*) 1<second> )
    let dynamicWaitScheduler = 0