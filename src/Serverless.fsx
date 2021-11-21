namespace Avestura.Serverless

#r "nuget: FSharp.Data"
#r "nuget: NodaTime"
open System
open System.IO
open System.Text.Json
open FSharp.Data

module General =
    let rnd = Random()


module DSExtensions =
    open General
    let russianRouletteMap inputMap =
        let mutable prevValue = 0.0
        inputMap |> Map.map (fun key value -> 
            let newValue = value + prevValue
            prevValue <- newValue
            newValue
        )

    let takeNFromRussianRouletteMap inputMap n =
        let rec takeNFromRussianRouletteMapRec (inputMap: Map<string, float>) n result =
            let mapLen = inputMap |> Map.count
            match n with
            | _ when n <=0 -> inputMap
            | n -> 
                let random = rnd.Next(0, 1)
                let (candidKey, candidValue) = 
                    inputMap |> Map.pick (fun k v -> if v >= random then Some (k, v) else None)
                let newMap = inputMap |> Map.remove candidKey
                let freedRange =
                    let inputMapKeys = inputMap |> Map.keys
                    let nextItemIndex = inputMapKeys |> Seq.findIndex (fun x -> x = candidKey) |> (+) 1
                    let nextItemKey = (inputMapKeys |> List.ofSeq).[nextItemIndex]
                    let nextItemValue = inputMap.[nextItemKey]
                    nextItemValue - candidValue
                takeNFromRussianRouletteMapRec newMap (n - 1) (result@[candidKey])

        takeNFromRussianRouletteMapRec inputMap n []

module PaperDataUtils = 
    let convertJsonValue (converter: JsonValue -> 'a) (props: ('b * JsonValue) array) =
        props |> Array.map(fun (key, value) -> (key, converter value)) |> Map.ofArray

    let convertKey (converter: string -> 'a) (props: (string * JsonValue) array) =
        props |> Array.map(fun (key, value) -> (converter key, value))


module PackagesData = 
    open PaperDataUtils
    type DependentUponDataType = JsonProvider<"../data/packageData/dependentUpon.json">
    type HitsRankDataType = JsonProvider<"../data/packageData/hitsrank.json">
    type PageRankDataType = JsonProvider<"../data/packageData/pagerank.json">

    let functionNames =
        DependentUponDataType.GetSample().JsonValue.Properties() |> Array.map(fun (x, y) -> x)

    let dependentUpon = 
        DependentUponDataType.GetSample().JsonValue.Properties() |> convertJsonValue (fun x -> x.AsInteger())
        
    let probOfDependency packageName =
        let sum = dependentUpon.Values |> Seq.sum |> float
        (float dependentUpon.[packageName]) / sum

    let probOfDependencies =
        let sum = dependentUpon.Values |> Seq.sum |> float
        dependentUpon |> Map.map (fun key value -> (float value) / sum)

    let hitsrank =
        HitsRankDataType.GetSample().JsonValue.Properties() |> convertJsonValue (fun x -> x.AsFloat())

    let pagerank =
        PageRankDataType.GetSample().JsonValue.Properties() |> convertJsonValue (fun x -> x.AsFloat())
    

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

type QueueFunctionRequest = {
    func: ServerlessFunction
    startTime: Duration
    serviceTime: LocalTime
}

module GenerateOptions =
    type Frequencies = bool * Map<string,Map<int,int>> option

    type Dependencies = bool * Map<string,float> option
    

module FunctionGenerator =
    open General
    let getNRandomFunctionNames n = 
        let allNames = PackagesData.functionNames
        allNames |> Array.sortBy(fun _ -> rnd.Next()) |> Array.take n

    let generateFunctionData (deps: GenerateOptions.Dependencies, n: int) =
        match deps with
        | false, _ ->
            let nNames = getNRandomFunctionNames n
            nNames |> Array.map (fun name -> {
                name = name
                deps = []
            })
        | true, Some data ->
            [||]
        | true, None ->
            [||]

    let generateFunctionQueueData () = 0