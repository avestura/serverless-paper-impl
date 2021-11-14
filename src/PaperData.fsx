#r "nuget: FSharp.Data"
namespace Avestura.Serverless.PaperData

open System.Text.Json
open FSharp.Data
open System.IO

module PaperDataUtils = 
    let convertJsonValue (converter: JsonValue -> 'a) (props: ('b * JsonValue) array) =
        props |> Array.map(fun (key, value) -> (key, converter value)) |> Map.ofArray

    let convertKey (converter: string -> 'a) (props: (string * JsonValue) array) =
        props |> Array.map(fun (key, value) -> (converter key, value))


module Packages = 
    open PaperDataUtils
    type DependentUponDataType = JsonProvider<"../data/packageData/dependentUpon.json">
    type HitsRankDataType = JsonProvider<"../data/packageData/hitsrank.json">
    type PageRankDataType = JsonProvider<"../data/packageData/pagerank.json">


    let dependentUpon = 
        DependentUponDataType.GetSample().JsonValue.Properties() |> convertJsonValue (fun x -> x.AsInteger())
        

    let hitsrank =
        HitsRankDataType.GetSample().JsonValue.Properties() |> convertJsonValue (fun x -> x.AsFloat())

    let pagerank =
        PageRankDataType.GetSample().JsonValue.Properties() |> convertJsonValue (fun x -> x.AsFloat())
    

module Frequencies =
    open PaperDataUtils
    type FreqDataType = JsonProvider<"../data/frequencies/freqData.json">

    let frequencies = 
        FreqDataType.GetSample().JsonValue.Properties()
        |> convertJsonValue (fun x -> x.Properties() |> convertKey int |> convertJsonValue (fun y -> y.AsInteger()))
