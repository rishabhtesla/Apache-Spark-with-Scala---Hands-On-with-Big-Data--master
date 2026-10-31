// Functions

// Function shape: def name(parameter: InputType): ReturnType = body.
// The return type can often be inferred, but writing it helps while learning.

def squareIt(x: Int) : Int = {
  x * x
}

def cubeIt(x : Int) : Int = {x * x * x}

println(squareIt(2))

println(cubeIt(3))

def transformInt(x: Int, f: Int => Int): Int = {
  // Int => Int is a function that accepts an Int and returns an Int.
  // Calling f(x) applies the function that the caller supplied.
  f(x)
}

val result = transformInt(2, cubeIt)
println(result)

// x => ... is an anonymous function (lambda) with an inferred parameter type.
transformInt(3, x => x * x * x)

transformInt(10, x => x / 2)

transformInt(2, x => {val y = x * 2; y * y})

// EXERCISE
// Strings have a built-in .toUpperCase method. For example, "foo".toUpperCase gives you back FOO.
// Write a function that converts a string to upper-case, and use that function of a few test strings.
// Then, do the same thing using a function literal instead of a separate, named function.
