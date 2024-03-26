module Main exposing (..)

import Browser
import Html exposing (Html, div, button, text, input)
import Html.Events exposing (onClick, onInput)
import Debug exposing (toString)
import Html.Attributes exposing (class)
import Html.Attributes exposing (placeholder)
import Html.Attributes exposing (value)
import Html.Attributes exposing (type_)
import Html.Attributes exposing (style)
import Http
import Json.Decode exposing (Decoder, map4, field, int, string)

type Fetching = Failure | Success | Loading

type alias Quote =
  {
    quote: String
    , source : String
    , author : String 
    , year : Int
  }

type alias Model =
  {
    count : Int
    , content: String
    , password: String
    , passwordAgain: String
    , text: String
    , quote : Quote
    , fetchingPublicOpinion: Fetching
    , fetchingQuote: Fetching
  }
initModel: () -> (Model, Cmd Msg)
initModel _ = 
  let
    getRandomQuoteCmd = getRandomQuote
    getTextCmd = Http.get { url = "https://elm-lang.org/assets/public-opinion.txt", expect = Http.expectString GotText }
  in
    (
      {
        count = 0
        , content = ""
        , password = ""
        , passwordAgain = ""
        , text = ""
        , quote = Quote "" "" "" 0
        , fetchingPublicOpinion = Loading
        , fetchingQuote = Loading
      }
      , Cmd.batch [getRandomQuoteCmd, getTextCmd]
    )

getRandomQuote : Cmd Msg
getRandomQuote = 
  Http.get { url = "https://elm-lang.org/api/random-quotes", expect = Http.expectJson GotQuote quoteDecoder }


quoteDecoder : Decoder Quote
quoteDecoder = 
  map4 Quote
    (field "quote" string)
    (field "source" string)
    (field "author" string)
    (field "year" int)

type Msg = 
  Increment 
  | Decrement 
  | DoubleIncrement
  | Change String -- we can add a type to a msg type
  | ChangePassword String
  | ChangePasswordAgain String
  | GotText (Result Http.Error String)
  | GotQuote (Result Http.Error Quote)
  | MoreQuotePlease

update: Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
    Increment ->
      ({ model | count = model.count + 1 }, Cmd.none)
    Decrement ->
      ({ model | count = model.count - 1 }, Cmd.none)
    DoubleIncrement ->
      ({ model | count = model.count + 2 }, Cmd.none)
    Change newContent ->
      ({ model | content = newContent }, Cmd.none)
    ChangePassword newPassword ->
      ({ model | password = newPassword }, Cmd.none)
    ChangePasswordAgain newPasswordAgain ->
      ({ model | passwordAgain = newPasswordAgain }, Cmd.none)
    GotText result ->
      case result of 
        Ok newText ->
          ({ model | text = newText, fetchingPublicOpinion = Success }, Cmd.none)
        Err _ ->
          ({ model | text = "Error", fetchingPublicOpinion = Failure }, Cmd.none)
    GotQuote result ->
      case result of 
        Ok newQuote ->
          ({ model | quote = newQuote, fetchingQuote = Success }, Cmd.none)
        Err _ ->
          ({ model | quote = Quote "" "" "" 0, fetchingQuote = Failure }, Cmd.none)
    MoreQuotePlease ->
      (model, getRandomQuote)


view model =
  div [ class "w-full flex flex-row justify-center items-center bg-red-500" ]
    [
      button [ class "bg-red-500 rounded", onClick Decrement ] [ text "-" ]
      , div [] [ text ( toString model.count ) ]
      , button [ onClick Increment ] [ text "+" ]
      , button [ onClick DoubleIncrement ] [ text "+" ]
      , input [ placeholder "type something", onInput Change, value model.content ] []
      , div [] [ text (String.reverse model.content) ]
      , div [] [
        viewInput "password" "password" ChangePassword model.password
        , viewInput "password" "password again" ChangePasswordAgain model.passwordAgain
      ]
      , div [] [
        if model.password == model.passwordAgain then
          div [ style "color" "green" ][ text "passwords match" ]
          
        else
          div [ style "color" "red" ][ text "passwords do not match"]
      ]
      , div [

      ] [
        case model.fetchingQuote of
          Loading ->
            text "Loading..."
          Success ->
            text model.quote.quote
          Failure ->
            text "Error"
        , button [ onClick MoreQuotePlease ] [ text "more please" ]
      ]
      , div [

      ] [
        case model.fetchingPublicOpinion of
          Loading ->
            text "Loading..."
          Success ->
            text model.text
          Failure ->
            text "Error"
      ]
    ]


viewInput : String -> String -> (String -> Msg) -> String -> Html Msg
viewInput typeInput labelPlaceholder onMsg valueInput =
  input [ type_ typeInput, placeholder labelPlaceholder, onInput onMsg, value valueInput ] []

subscriptions : Model -> Sub Msg
subscriptions model =
  Sub.none


main = 
  Browser.element 
    { init = initModel
    , update = update
    , view = view
    , subscriptions = subscriptions }

