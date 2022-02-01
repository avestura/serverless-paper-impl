namespace Avestura.Serverless

open NodaTime
open System.Text.Json
open System.Text.Json.Serialization
open System.Collections.Generic
open MathNet.Numerics.Distributions
open System.Security.Cryptography
open MathNet.Numerics
open NodaTime
open NodaTime
open NodaTime

#r "nuget: FSharp.Data"
#r "nuget: NodaTime"
#r "nuget: MathNet.Numerics"
#r "nuget: MathNet.Numerics.FSharp"
#r "nuget: Dijkstra.NET"

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
[<Measure>] type hour

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

    let PurgeMergeStatusesBeforeInHours = 1.<hour>

    let ContainerCreationCostInSeconds_DefaultChiSquareFreedom = 5.0

    let inline weilbullStretched x a b = 1. - (Math.E ** (-1. * ((x/a) ** b)))

    /// Returns a number between 0. and 1.
    let inline normalize x = min 1. (weilbullStretched x 5. 2.)

    let inline bytesToKilobytes (x: float<dbyte>) = x / (1000.0<dbyte/kilobyte>)
    let inline kilobytesToMegabytes (x: float<kilobyte>) = x / (1000.0<kilobyte/megabyte>)
    let bytesToMegabytes = bytesToKilobytes >> kilobytesToMegabytes

    let calculateRestorationTime (size: float<dbyte>) =
        let mb = bytesToMegabytes size
        let sec = PackageRestoreSpeed * mb

        let strippedSec = float sec 

        Sample.normal strippedSec (strippedSec / 5.) rnd |> abs |> ceil |> int64 |> (*) 1L<second>

    let calculateRestorationDuration (size: float<dbyte>) =
        size |> calculateRestorationTime |> int64 |> Duration.FromSeconds

module NodaTimeUtils =
    open NodaTime
    let addDuration (d: Duration) (lt: LocalTime) =
        lt.PlusTicks(d.TotalTicks |> int64)

    let avgDurations (durations: Duration list) =
        durations
        |> List.averageBy (fun x -> x.TotalSeconds)
        |> Duration.FromSeconds


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

module GraphExtensions =
    open Dijkstra.NET.Graph
    open Dijkstra.NET.ShortestPath
    open System.Linq

    let printAdjMatrix (adj: uint[,]) =
        printfn "--------------"
        for x in 0 .. (Array2D.length1 adj - 1) do
          printf "| "
          for y in 0 .. (Array2D.length2 adj - 1) do
            printf " %d |" adj.[x,y]
          printfn "\n--------------"

    let dijkstra (nodes : uint32 seq) edges src dst = 
        let src', dst' = src + 1u, dst  + 1u
        let g = new Graph<int, string>()
        let idMapper = new Dictionary<uint, uint>()
        for n in nodes do
            let nodeId = g.AddNode(int n)
            idMapper.Add (n, nodeId)
        let inversedIdMapper = idMapper.ToDictionary ((fun x -> x.Value), (fun x -> x.Key))
        for (n1, n2, w) in edges do
            let sourceId = idMapper[n1]
            let destinationId = idMapper[n2]
            g.Connect(sourceId, destinationId, w, $"{n1} to {n2} with cost {w}") |> ignore
        let result = g.Dijkstra(src', dst')
        result.GetPath() |> Seq.toList |> List.map (fun x -> inversedIdMapper[x]), result.Distance

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

type ServerlessFunction =  {
    id: uint
    name: string
    deps: string list
}

[<RequireQualifiedAccess>]
module ServerlessFunction =
    let getFullDependencySize func =
        func.deps
        |> List.map PackagesData.getFullPackageDataSafe 
        |> List.map (fun x -> x.install.bytes)
        |> List.sum
        |> (*) 1<dbyte>

    let getNonCachedDependencySize (cachedFunc: ServerlessFunction) currentFunction =
        let notCachedDeps = DSExtensions.listSubtract currentFunction.deps cachedFunc.deps
        notCachedDeps
        |> List.map PackagesData.getFullPackageDataSafe 
        |> List.map (fun x -> x.install.bytes)
        |> List.sum
        |> (*) 1<dbyte>

    let fullRestorationDuration func = 
        getFullDependencySize func
        |> float
        |> (*) 1.0<dbyte>
        |> General.calculateRestorationDuration

    let nonCachedRestorationDuration (cachedFunc: ServerlessFunction) currentFunction =
        currentFunction
        |> getNonCachedDependencySize cachedFunc
        |> float
        |> (*) 1.0<dbyte>
        |> General.calculateRestorationDuration

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

type GeneralizedServerlessFunction = InferedFromCoopContext | Concrete of ServerlessFunction
with
    member this.concrete() =
        match this with
        | Concrete f -> f
        | InferedFromCoopContext -> failwith "concerete serverelss function isn't available because this function is infered"


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
            [1u..n] |> List.map (fun i -> {
                id = i - 1u
                name = $"f{i}"
                deps = nNames |> List.ofArray
            })
        | DataUnawareRandomNormal (m, s) ->
            let c = Normal.Sample(float m, float s) |> abs |> ceil |> int
            let names = getNRandomPackageNames c
            [1u..n] |> List.map (fun i -> {
                id = i - 1u
                name = $"f{i}"
                deps = names |> List.ofArray
            })
            
        | DataAwareRandomUniform (maybeData, c) ->
            let data = 
                match maybeData with
                | Some data -> data
                | None -> PackagesData.dependentUpon

            let probData = makeProbMap data

            [1u..n] |> List.map (fun i -> {
                id = i - 1u
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

            [1u..n] |> List.map (fun i -> {
                id = i - 1u
                name = $"f{i}"
                deps = probData |> takeNFromProbMap c
            })

type CoopNetworkTemplate(numberOfFunctions: int) =
    let n = numberOfFunctions
    let graph = Array2D.init n n (fun _ _ -> General.rnd.NextDouble())
    member _.Edge m n = graph[m, n]


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

    let defaultUnawareRandomUniformDepsCount = DataUnawareRandomUniform NumberOfFunctionDeps_DefaultNormalDistMean
    let defaultUnawareRandomNormalDepsCount = DataUnawareRandomNormal (NumberOfFunctionDeps_DefaultNormalDistMean, NumberOfFunctionDeps_DefaultNormalDistStdDev)

    let defaultAwareRandomUniformDepsCount = DataAwareRandomUniform (None, NumberOfFunctionDeps_DefaultNormalDistMean)
    let defaultAwareRandomNormalDepsCount = DataAwareRandomNormal (None, NumberOfFunctionDeps_DefaultNormalDistMean, NumberOfFunctionDeps_DefaultNormalDistStdDev)
    type Options = FrequencyOfInvocationMode * FunctionCoopMode * FunctionGenerateOptions.DependenciesCount


type QueueFunctionRequest = {
    func: GeneralizedServerlessFunction
    startTime: LocalTime
    serviceTime: Duration
}
with
    member this.concreteFunc() = this.func.concrete()


type QueueFunctionRequestBatch = {
    funcs: ServerlessFunction list
    requests: QueueFunctionRequest list
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

    let generateFunctionQueueData (options: QueueFunctionGeneration.Options) numberOfFunctions = 
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
                    | IgnoreCoopNetwork -> Concrete (pickRandomItemFromList funcList)
                    | UseCoopNetwork -> InferedFromCoopContext // TODO: must be fixed

                let newItem = {
                    func = nextFunctionToPick
                    startTime = startTime
                    serviceTime = Duration.FromSeconds serviceTime
                }

                generate startTime (result@[newItem])
            
        { funcs = funcList; requests = generate (LocalTime(0,0,0)) [] }
        
type Container = {
    name: string
    id: uint
    runningFunction: ServerlessFunction option
    created: LocalTime
    creationDuration: Duration
    finishedExecTime: LocalTime option
    disposed: LocalTime option
    totalWorkingDuration: Duration
    totalWaitForNextFunctionDuration: Duration
    totalRestorationDuration: Duration
    chanceOfWaitingMore: float
    chanceOfWaitingMoreMaximumValue: float
    previousFunctions: ServerlessFunction list
    mountKey: Guid // A key to keep track of changes in the container status of being occupied by different functions
}

[<RequireQualifiedAccess>]
module Container =
    let makeNew name id func created =
        {
            name = name
            id = id
            runningFunction = func
            created = created
            creationDuration = Duration.Zero
            finishedExecTime = None
            disposed = None
            totalWorkingDuration = Duration.Zero
            totalWaitForNextFunctionDuration = Duration.Zero
            totalRestorationDuration = Duration.Zero
            chanceOfWaitingMore = 0.0
            chanceOfWaitingMoreMaximumValue = 1.0
            previousFunctions = []
            mountKey = Guid.NewGuid()
        }
    let isDisposed cnt = cnt.disposed.IsSome
    let numberOfTimesUsed cnt = cnt.previousFunctions |> List.length
    let isWaitingForNextFunction cnt = cnt.finishedExecTime.IsSome && (isDisposed cnt|> not)
    let terminate disposeTime cnt =
        {
            cnt with 
                disposed = Some disposeTime
                finishedExecTime = Some disposeTime
                runningFunction = None
                previousFunctions = match cnt.runningFunction with Some x -> cnt.previousFunctions@[x] | _ -> cnt.previousFunctions
                mountKey = Guid.NewGuid()
        }
    let finishExec finishTime cnt =
        { cnt with finishedExecTime = Some finishTime; mountKey = Guid.NewGuid() }

    let randomCreationDuration () =
        General.rnd
        |> Sample.chiSquared General.ContainerCreationCostInSeconds_DefaultChiSquareFreedom
        |> Duration.FromSeconds

type TimelineEventKind =
    | QueueRequest of QueueFunctionRequest // A new function wants to run in the serverless platform
    | FinishRunningFunction of Container // A function finished its execution in container
    | MountWaitTimedOut of Container * mountKey: Guid // Container waited some time epoch for another function to join but nothing happened
    | EvaporateCoopNetEdges

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

[<RequireQualifiedAccess>]
module TimelineEvent =
    let insertEvent (item: TimelineEvent) (evList: TimelineEvent list) =
        DSExtensions.insertSorted item evList

type DependencyGraph = {
    adjMatrix: uint[,]
    numberOfFunctions: uint
}

[<RequireQualifiedAccess>]
module DependencyGraph =
    let init numberOfFunctions = 
        let l = int numberOfFunctions
        let graph = Array2D.init l l (fun _ _ -> if General.rnd.NextDouble() < 0.5 then 0u else General.rnd.Next(1, 1000) |> uint)
        { adjMatrix = graph; numberOfFunctions = numberOfFunctions }

    let incr n m graph =
        let newAdjMatrix =  Array2D.copy graph.adjMatrix
        newAdjMatrix[n, m] <- newAdjMatrix[n, m] + 1u

        { graph with adjMatrix = newAdjMatrix }
        
    let incrAll list graph =
        let newAdjMatrix = Array2D.copy graph.adjMatrix
        for (n, m) in list do
            let n', m' = int n, int m
            newAdjMatrix[n', m'] <- newAdjMatrix[n', m'] + 1u

        { graph with adjMatrix = newAdjMatrix }

    let evaporate graph =
        let newArray =
            graph.adjMatrix
            |> Array2D.map (fun x -> max 0u (x - 1u))
            
        { graph with adjMatrix = newArray }

    let edgesOf verticeId graph =
        graph.adjMatrix[verticeId, *]

    let neighbourIds verticeId graph =
        graph |> edgesOf verticeId
        |> Array.indexed
        |> Array.filter (fun (_, value) -> value > 0u)
        |> Array.map (fun (index, _) -> index)

    let dijkestraPath sourceId destinationId graph =
        let n = int graph.numberOfFunctions
        let nodes = seq {
            for i = 0 to n-1 do
                yield (uint i)
        }
        let edges =
            seq {
                for i = 0 to n-1 do
                    for j = 0 to n-1 do
                        let v = graph.adjMatrix[i, j]
                        if i <> j && v <> 0u then yield (uint i, uint j, int v)
            }
            
        GraphExtensions.dijkstra nodes edges sourceId destinationId

    let edgeWeightsFromPath (path: uint list) graph =
        let adj = graph.adjMatrix

        let rec weights currentPath result =
            match currentPath with
            | [] -> result
            | x::y::rest ->
                let newResult = result@[adj[int x, int y]]
                let newPath = y::rest
                weights newPath newResult
            | x::[] -> result

        weights path []

    let dijkestra sourceId destinationId graph =
        let path, distance = graph |> dijkestraPath sourceId destinationId
        let weights = graph |> edgeWeightsFromPath path

        path, weights, distance

    let coopScore f1Id f2Id graph =
        let rec score p edgeWeights =
            match edgeWeights with
            | [] -> p
            | currentEdge::restEdges ->
                let newP = p * (float currentEdge |> General.normalize)
                score newP restEdges

        let path, distance = graph |> dijkestraPath f1Id f2Id
        let weights = graph |> edgeWeightsFromPath path

        match weights with [] -> 0. | xs -> score 1. xs

type MergeStatusKind = SuccessfulMerge | FailedMerge
type MergeStatus = MergeStatusKind * LocalTime

type SimulatorContext = {
    day: string
    events: TimelineEvent list
    containerIdCounter: uint
    containers: Map<string, Container>
    dependencyGraph: DependencyGraph
    functionsMergeStatuses: Map<uint, MergeStatus list>
}

[<RequireQualifiedAccess>]
module SimulatorContext =
    open General
    let getMountableContainers ctx =
        ctx.containers.Values
        |> List.ofSeq
        |> List.filter Container.isWaitingForNextFunction

    let getRunningFunctions ctx =
        ctx.containers.Values
        |> List.ofSeq
        |> List.filter (fun x -> not(Container.isDisposed x) && not(Container.isWaitingForNextFunction x))
        |> List.map (fun x -> x.runningFunction)
        //|> List.filter (fun x -> x.IsSome)
        |> List.distinctBy (fun x -> x.Value.name)
        |> List.map (fun x -> x.Value)

    let updateContainer cont ctx =
        { ctx with
            containers = ctx.containers |> Map.add cont.name cont
        }

    let updateDependencyGraph newGraph ctx =
        { ctx with dependencyGraph = newGraph }

    let insertEvent ev ctx =
        { ctx with 
            events = ctx.events |> TimelineEvent.insertEvent ev
        }

    let private addNewFunctionMergeStatus functionId time kind ctx =
        let mergeStatus = ctx.functionsMergeStatuses[functionId]
        let newItem = kind, time
        { ctx with
            functionsMergeStatuses = ctx.functionsMergeStatuses |> Map.add functionId (mergeStatus@[newItem])
        }

    let functionMergeFailed functionId time ctx =
        ctx |> addNewFunctionMergeStatus functionId time FailedMerge

    let functionMergeSuccesfull functionId time ctx =
        ctx |> addNewFunctionMergeStatus functionId time SuccessfulMerge
        
    let purgeOldMergeStatuses now ctx =
        let isNew ((_, time): MergeStatus) =
            (now - time).ToDuration().TotalHours <= (float PurgeMergeStatusesBeforeInHours)

        let newMergeStatuses =
            seq {
                for item in ctx.functionsMergeStatuses do
                    let newItems = item.Value |> List.filter isNew
                    yield item.Key, newItems
            }
            |> Map.ofSeq

        { ctx with functionsMergeStatuses = newMergeStatuses }

    let functionMergeSuccessRate functionId ctx =
        let statuses = ctx.functionsMergeStatuses[functionId]
        let success = statuses |> List.filter (fun (k, _) -> k = SuccessfulMerge) |> List.length |> float
        let total = statuses |> List.length |> float
        
        success / total

    let coopScore fSourceId fDestId ctx =
        ctx.dependencyGraph|> DependencyGraph.coopScore fSourceId fDestId

    let getContainers ctx =
        Map.values ctx.containers |> List.ofSeq

    let getTotalTimes ctx =
        let cnts = getContainers ctx
        let creation = cnts |> List.sumBy (fun x -> x.creationDuration)
        let working = cnts |> List.sumBy (fun x -> x.totalWorkingDuration)
        let waiting = cnts |> List.sumBy (fun x -> x.totalWaitForNextFunctionDuration)
        let restoration = cnts |> List.sumBy (fun x -> x.totalRestorationDuration)

        (creation, working, waiting, restoration)


type Scheduler = TimelineEvent -> SimulatorContext -> SimulatorContext

module Simulator =
    open DSExtensions
    let insertEvent = TimelineEvent.insertEvent
    let rec runSimulation (scheduler: Scheduler) (context: SimulatorContext) =
        match context.events with
        | [] -> context
        | event::restEvents -> 
            let popEventContext = { context with events = restEvents }
            let newContext = scheduler event popEventContext
            runSimulation scheduler newContext

    let convertQueueRequestDataToSimulatorContext (day: string) (queueFuncReqBatch: QueueFunctionRequestBatch) =
        let funcLen = uint queueFuncReqBatch.funcs.Length
        {
            day = day
            events =
                queueFuncReqBatch.requests
                |> List.map (fun qfr -> { time = qfr.startTime ; kind = QueueRequest qfr })
                |> insertEvent ({ time = LocalTime(0,0,0) ; kind = EvaporateCoopNetEdges })
            containerIdCounter = 0u
            containers = Map.empty
            dependencyGraph = DependencyGraph.init funcLen
            functionsMergeStatuses =
                queueFuncReqBatch.funcs
                |> List.map (fun f -> f.id, [])
                |> Map.ofList
        }

    let runSimulatonWithQueueData (day: string) (queueFuncData: QueueFunctionRequestBatch) (scheduler: Scheduler) =
        let ctx = convertQueueRequestDataToSimulatorContext day queueFuncData
        runSimulation scheduler ctx

module BasicSchedulers =
    open General
    open Simulator
    let doNothingScheduler : Scheduler = fun _ ctx -> ctx
    let noMergeScheduler: Scheduler = fun ev ctx ->
        let (time, kind) = ev.time, ev.kind
        match kind with
        | QueueRequest qfr ->
            let f = qfr.concreteFunc()
            let restoreDuration = ServerlessFunction.fullRestorationDuration f
            let containerCreationDuration = Container.randomCreationDuration()
            let lifetimeDuration = containerCreationDuration + restoreDuration + qfr.serviceTime
            let functionFinish = time.PlusNanoseconds(lifetimeDuration.ToInt64Nanoseconds())
            let newContainer = {
                name = $"container-%d{ctx.containerIdCounter}"
                id = ctx.containerIdCounter
                runningFunction = Some f
                created = time
                creationDuration = containerCreationDuration
                finishedExecTime = None
                disposed = None
                totalRestorationDuration = restoreDuration
                totalWaitForNextFunctionDuration = Duration.Zero
                totalWorkingDuration = qfr.serviceTime
                chanceOfWaitingMore = 0.0
                chanceOfWaitingMoreMaximumValue = 1.0
                previousFunctions = []
                mountKey = Guid.NewGuid()
            }
            { ctx with
                containerIdCounter = ctx.containerIdCounter + 1u
                containers = ctx.containers |> Map.add newContainer.name newContainer
                events = ctx.events |> insertEvent {
                    time = functionFinish
                    kind = FinishRunningFunction newContainer
                }
            }
        | FinishRunningFunction container -> 
            let terminatedContainer = container |> Container.terminate ev.time
            let newConts = ctx.containers |> Map.add container.name terminatedContainer
            { ctx with containers = newConts }
        | _ -> ctx

    let computedWaitScheduler (getWaitTime: unit -> int<second>): Scheduler = fun ev ctx ->
        let (time, kind) = ev.time, ev.kind
        match kind with
        | QueueRequest qfr ->
            let mountConts = ctx |> SimulatorContext.getMountableContainers
            match mountConts with
            | [] ->
                let f = qfr.concreteFunc()
                let restoreDuration =  ServerlessFunction.fullRestorationDuration f
                let containerCreationDuration = Container.randomCreationDuration()
                let lifetimeDuration = containerCreationDuration + restoreDuration + qfr.serviceTime
                let functionFinish = time.PlusNanoseconds(lifetimeDuration.ToInt64Nanoseconds())
                let newContainer = {
                    name = $"container-%d{ctx.containerIdCounter}"
                    id = ctx.containerIdCounter
                    runningFunction = Some f
                    created = time
                    creationDuration = containerCreationDuration
                    finishedExecTime = None
                    disposed = None
                    totalRestorationDuration = restoreDuration
                    totalWaitForNextFunctionDuration = Duration.Zero
                    totalWorkingDuration = qfr.serviceTime
                    chanceOfWaitingMore = 0.0
                    chanceOfWaitingMoreMaximumValue = 1.0
                    previousFunctions = []
                    mountKey = Guid.NewGuid()
                }
                { ctx with
                    containerIdCounter = ctx.containerIdCounter + 1u
                    containers = ctx.containers |> Map.add newContainer.name newContainer
                    events = ctx.events |> insertEvent {
                        time = functionFinish
                        kind = FinishRunningFunction newContainer
                    }
                }
            | conts ->
                let f = qfr.concreteFunc()
                let host = DSExtensions.pickRandomItemFromList conts
                let restorationTime = f |> ServerlessFunction.nonCachedRestorationDuration host.runningFunction.Value
                let waitTime = (time - host.finishedExecTime.Value).ToDuration()
                let finishTime = time.PlusNanoseconds((restorationTime + qfr.serviceTime).ToInt64Nanoseconds())
                let modifiedHost = {
                    host with
                        runningFunction = Some f
                        finishedExecTime = None
                        totalRestorationDuration = host.totalRestorationDuration + restorationTime
                        totalWaitForNextFunctionDuration = host.totalWaitForNextFunctionDuration + waitTime
                        totalWorkingDuration = host.totalWorkingDuration + qfr.serviceTime
                        previousFunctions = host.previousFunctions @ [host.runningFunction.Value]
                        mountKey = Guid.NewGuid()
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
            let nextEventTime = time.PlusSeconds(getWaitTime() |> int64)
            let newKey = Guid.NewGuid()
            let newCont = {
                cont with
                    finishedExecTime = Some time
                    mountKey = newKey
            }
            { ctx with
                containers = ctx.containers |> Map.add newCont.name newCont
                events = ctx.events |> insertEvent {
                    time = nextEventTime
                    kind = MountWaitTimedOut (newCont, newKey)
                }
            }
        | MountWaitTimedOut (cont, key) ->
            let targetCont = ctx.containers.[cont.name]
            if key = targetCont.mountKey then
                let terminatedCont = targetCont |> Container.terminate time
                { ctx with
                    containers = ctx.containers |> Map.add terminatedCont.name terminatedCont
                }
            else
                ctx
        | EvaporateCoopNetEdges -> ctx

    let staticWaitScheduler (waitTime: int<second>): Scheduler =
        computedWaitScheduler (fun () -> waitTime)

    let randomWaitScheduler (mean: int<second>) (stddev: int<second>): Scheduler =
        computedWaitScheduler (fun () -> Normal.Sample (float mean, float stddev) |> ceil |> int |> (*) 1<second>)


module ComplexSchedulers =
    open General
    open Simulator

    type RLAction = 
        | WaitMore of chanceDiff: float 
        | WaitLess of chanceDiff: float
        | Neutral
        | Terminate

    type RLPolicy = SimulatorContext -> Container -> RLAction

    type DynamicSchedulerConfiguration = {
        stepCost: float
        maxWaitChanceCost: float
        waitTimeoutSeconds: int
    }

    let defaultDynamicSchedulerConfig = {
        stepCost = 0.05
        maxWaitChanceCost = 0.01
        waitTimeoutSeconds = 10
    }

    let dynamicWaitScheduler (policy: RLPolicy) (config: DynamicSchedulerConfiguration option): Scheduler = fun ev originalContext ->
        let (time, kind) = ev.time, ev.kind
        let ctx = originalContext |> SimulatorContext.purgeOldMergeStatuses time
        let conf = match config with Some c -> c | None -> defaultDynamicSchedulerConfig
        match kind with
        | QueueRequest qfr ->
            let f = qfr.concreteFunc()
            let mountConts = ctx |> SimulatorContext.getMountableContainers
            let running = ctx |> SimulatorContext.getRunningFunctions
            let incrementEdgeValuesSeq = seq {
                for r in running do
                    yield (r.id, f.id)
            }
            let newDependencyGraph = ctx.dependencyGraph |> DependencyGraph.incrAll incrementEdgeValuesSeq
            match mountConts with
            | [] ->
                let restoreDuration = ServerlessFunction.fullRestorationDuration f
                let containerCreationDuration = Container.randomCreationDuration()
                let lifetimeDuration = containerCreationDuration + restoreDuration + qfr.serviceTime
                let functionFinish = time.PlusNanoseconds(lifetimeDuration.ToInt64Nanoseconds())
                let newContainer = {
                    name = $"container-%d{ctx.containerIdCounter}"
                    id = ctx.containerIdCounter
                    runningFunction = Some f
                    created = time
                    creationDuration = containerCreationDuration
                    finishedExecTime = None
                    disposed = None
                    totalRestorationDuration = restoreDuration
                    totalWaitForNextFunctionDuration = Duration.Zero
                    totalWorkingDuration = qfr.serviceTime
                    chanceOfWaitingMore = 1.0
                    chanceOfWaitingMoreMaximumValue = 1.0
                    previousFunctions = []
                    mountKey = Guid.NewGuid()
                }
                { ctx with
                    containerIdCounter = ctx.containerIdCounter + 1u
                    containers = ctx.containers |> Map.add newContainer.name newContainer
                    events = ctx.events |> insertEvent {
                        time = functionFinish
                        kind = FinishRunningFunction newContainer
                    }
                    dependencyGraph = newDependencyGraph
                }
            | conts ->
                let host =
                    conts |>
                    List.maxBy (fun c -> ServerlessFunction.similarity f (c.runningFunction.Value))
                let restorationTime = f |> ServerlessFunction.nonCachedRestorationDuration host.runningFunction.Value
                let waitTime = (time - host.finishedExecTime.Value).ToDuration()
                let finishTime = time.PlusNanoseconds((restorationTime + qfr.serviceTime).ToInt64Nanoseconds())
                let modifiedHost = {
                    host with
                        runningFunction = Some f
                        finishedExecTime = None
                        totalRestorationDuration = host.totalRestorationDuration + restorationTime
                        totalWaitForNextFunctionDuration = host.totalWaitForNextFunctionDuration + waitTime
                        totalWorkingDuration = host.totalWorkingDuration + qfr.serviceTime
                        previousFunctions = host.previousFunctions @ [host.runningFunction.Value]
                        mountKey = Guid.NewGuid()
                }

                ctx
                    |> SimulatorContext.updateContainer modifiedHost
                    |> SimulatorContext.insertEvent { time = finishTime; kind = FinishRunningFunction modifiedHost }
                    |> SimulatorContext.updateDependencyGraph newDependencyGraph
                    |> SimulatorContext.functionMergeSuccesfull host.runningFunction.Value.id time


        | FinishRunningFunction cont ->
            let nextEventTime = time.PlusSeconds(conf.waitTimeoutSeconds)
            let newKey = Guid.NewGuid()
            let newCont = {
                cont with
                    finishedExecTime = Some time
                    mountKey = newKey
            }
            { ctx with
                containers = ctx.containers |> Map.add newCont.name newCont
                events = ctx.events |> insertEvent {
                    time = nextEventTime
                    kind = MountWaitTimedOut (newCont, newKey)
                }
            }
        | MountWaitTimedOut (cont, key) ->
            let targetCont = ctx.containers.[cont.name]
            if key <> targetCont.mountKey then ctx
            else
                let action = policy ctx targetCont
                let newMaxWaitChance = max 0. (cont.chanceOfWaitingMoreMaximumValue - conf.maxWaitChanceCost)
                let newWaitMoreOdds = max 0. (cont.chanceOfWaitingMore - conf.stepCost)

                let shouldTerminate odds =
                    let rndValue = General.rnd.NextDouble()
                    if (odds <= 0.) || (rndValue <= odds) then true else false

                let inline between minValue maxValue value = value |> max minValue |> min maxValue

                let terminatedContext diff = 
                    let terminatedCont = 
                        { Container.terminate time targetCont with
                            chanceOfWaitingMore = newWaitMoreOdds + diff |> between 0. newMaxWaitChance
                            chanceOfWaitingMoreMaximumValue = newMaxWaitChance
                        }

                    ctx
                        |> SimulatorContext.updateContainer terminatedCont
                        |> SimulatorContext.functionMergeFailed (cont.runningFunction.Value.id) time

                let continueWaitingContext chanceDiff =
                    
                    let newCont =
                        let newChance = newWaitMoreOdds + chanceDiff |> between 0. newMaxWaitChance
                        { targetCont with
                            chanceOfWaitingMoreMaximumValue = newMaxWaitChance
                            chanceOfWaitingMore = newChance
                        }

                    let nextEventTime = time.PlusSeconds(conf.waitTimeoutSeconds)
                    let nextCheckEvent = { time = nextEventTime; kind = MountWaitTimedOut (newCont, key) }
                    let updatedCtx = ctx |> SimulatorContext.updateContainer newCont
                    { updatedCtx with 
                        events = ctx.events |> insertEvent nextCheckEvent
                    }

                match action with
                | Terminate -> terminatedContext 0.
                | Neutral ->
                    if shouldTerminate newWaitMoreOdds then terminatedContext 0.
                    else
                        continueWaitingContext 0.
                        
                | WaitMore chanceDiff ->
                    let newChance = newWaitMoreOdds + chanceDiff
                    if shouldTerminate newChance then terminatedContext chanceDiff
                    else continueWaitingContext chanceDiff
                | WaitLess chanceDiff ->
                    let newChance = newWaitMoreOdds - chanceDiff
                    if shouldTerminate newChance then terminatedContext (-chanceDiff)
                    else continueWaitingContext (-chanceDiff)
                    

        | EvaporateCoopNetEdges ->
            let newEvaporateEvent = { time = time.PlusMinutes(1); kind = EvaporateCoopNetEdges }
            { ctx with 
                dependencyGraph = DependencyGraph.evaporate ctx.dependencyGraph
                events = ctx.events |> insertEvent newEvaporateEvent
            }
module ProbabilityUtils =
    let unionOfIndependentEvents probs =
        let inversedProbs = probs |> List.map (fun x -> 1. - x)
        1. - (List.reduce (*) probs)

module DynamicSchedulerPolicies =
    open ComplexSchedulers
    

    let alwaysNeturalPolicy : RLPolicy = fun ctx cont ->
        Neutral

    let contextBasedPolicy : RLPolicy = fun ctx cont ->
        let rf = cont.runningFunction.Value // running function
        let prevSuccessRate = ctx |> SimulatorContext.functionMergeSuccessRate (rf.id) // probability
        let coopWithRunningFunctionsProbability = // probability
            ctx
            |> SimulatorContext.getRunningFunctions
            |> List.map (fun fSource -> ctx |> SimulatorContext.coopScore fSource.id rf.id) // array of probabilities
            |> ProbabilityUtils.unionOfIndependentEvents

        let similarityScoreProb = // probability
            ctx
            |> SimulatorContext.getRunningFunctions
            |> List.map (fun fSource -> ServerlessFunction.similarity fSource rf) // array of probs
            |> ProbabilityUtils.unionOfIndependentEvents

        let prob =
            [ prevSuccessRate; coopWithRunningFunctionsProbability; similarityScoreProb ]
            |> ProbabilityUtils.unionOfIndependentEvents

        match prob with
        | x when x > 1.0 -> failwith "Probability of context-based policy was calculated more than 1."
        | x when x < 0.  -> failwith "Probability of context-based policy was calculated less than 0."
        | x when x >= 0.45 || x <= 0.55 -> Neutral
        | x when x >= 0.55 -> WaitMore ((x - 0.55) / 3.)
        | x when x <= 0.45 -> WaitLess ((0.45 - x) / 3.)
        | _ -> failwith "never happens"

module EvaluatorConfigs =
    open QueueDataGenerator
    open QueueFunctionGeneration
    open Simulator

    type private opts = QueueFunctionGeneration.Options

    let igCoop = IgnoreCoopNetwork

    let config_IFD_UA_N : opts =
        (IgnoreFrequencyData, igCoop, defaultUnawareRandomNormalDepsCount)

    let config_IFD_UA_U : opts =
        (IgnoreFrequencyData, igCoop, defaultUnawareRandomUniformDepsCount)

    let config_IFD_AW_N : opts =
        (IgnoreFrequencyData, igCoop, defaultAwareRandomNormalDepsCount)

    let config_IFD_AW_U : opts =
        (IgnoreFrequencyData, igCoop, defaultAwareRandomUniformDepsCount)

    let config_UFD_UA_N : opts =
        (UseFrequencyData FrequenciesData.frequencies, igCoop, defaultUnawareRandomNormalDepsCount)

    let config_UFD_UA_U : opts =
        (UseFrequencyData FrequenciesData.frequencies, igCoop, defaultUnawareRandomUniformDepsCount)

    let config_UFD_AW_N : opts =
        (UseFrequencyData FrequenciesData.frequencies, igCoop, defaultAwareRandomNormalDepsCount)

    let config_UFD_AW_U : opts =
        (UseFrequencyData FrequenciesData.frequencies, igCoop, defaultAwareRandomUniformDepsCount)