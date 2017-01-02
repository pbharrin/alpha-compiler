# Alpha Compiler #
Generates Python code to be used on Quantopian from an Alpha equation.  
The code generated can be cut and pasted directly into your Quantopian
code.  For examples of how to use factors see the latest notebook posted [here](https://www.quantopian.com/posts/alpha-compiler#581939e7fb861562e4000246 "Alpha Complier Discussion on Quantopian").  The code genrates a custom factor named AlphaX, if you are going to use multiple generated here you will need to change the name of AlphaX to something unique for each factor.  

## Syntax ##
The syntax is inspired by the document [101 Formulaic Alphas](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2701346 "101 Alphas" ) by Zura Kakushadze.  I recommend you read that paper if you creating your own alphas.

### Inputs ###
The following can be used as inputs:

*   open, high, low, close, volume
*   returns
*   cap
*   vwap  It should be noted that Quantopian's VWAP is not computed the same way that the rest of the industry compues VWAP.  On Quantopian's platform VWAP is treated as another factor and uses daily price and volume.  
*   `adv*` where * is an integer.  For example: `adv8` computes the ADV over the last 8 days.  

## Unirary Operators ##

*   abs 
*   sign
*   rank

## Binary Operators ##

*   The usual infix algebraic operators can be used: `+,-,/,*`
*   Logical infix operators can be used as well: `>,<, ||, &&`
*   max
*   min
*   scale
*   signedpower
*   indneutralize

## Ternary Operator ##

*  A ? Y : Z  This is the C-style ternary operator that is shorthand for if A then Y else Z.  

## Time-Series Operators ##
The time-series operators as a special case of binary operators where the last argument is the number of days.  In the inspiration paper this could be any positive real number, but it this implementation the number needs to be a positive integer.  For example the `delay(sig, N)` operator will return a version of the `sig` data delayed N days.  

### Binary Time-Series Operators ###

*   delay 
*   delta
*   ts_min
*   ts_max
*   ts_argmin
*   ts_argmax
*   decay_linear
*   ts_rank
*   sum
*   product
*   stddev


### Ternary Time-Series Operators ###
*   correlation
*   covariance
