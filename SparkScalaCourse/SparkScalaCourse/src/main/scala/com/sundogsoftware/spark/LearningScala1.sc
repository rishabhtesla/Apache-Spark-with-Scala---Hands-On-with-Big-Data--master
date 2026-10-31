   // val prevents reassignment of the name hello. Scala Strings are immutable,
   // so concatenation creates a new String rather than changing the old one.
   val hello: String = "Hola!"

   // var allows this name to be assigned a different value later.
   var helloThere: String = hello
   helloThere = hello + " There!"
   println(helloThere)

   val immutableHelloThere = hello + " There"
   println(immutableHelloThere)

   // Common Scala primitive types. The annotation after ":" is optional when
   // Scala can infer the type from the value on the right-hand side.

   val numberOne: Int = 1
   val truth: Boolean = true
   val letterA: Char = 'a'
   val pi: Double = 3.14159265
   val piSinglePrecision: Float = 3.14159265f
   val bigNumber: Long = 123456789
   val smallNumber: Byte = 127

   println("Here is a mess: " + numberOne + truth + letterA + pi + bigNumber)

   // s-interpolation inserts values; f-interpolation also applies formatting.
   println(f"Pi is about $piSinglePrecision%.3f")
   println(f"Zero padding on the left: $numberOne%05d")

   println(s"I can use the s prefix to use variables like $numberOne $truth $letterA")

   println(s"The s prefix isn't limited to variables; I can include any expression. Like ${1+2}")

   val theUltimateAnswer: String = "To life, the universe, and everything is 42."
   // Triple-quoted strings avoid escaping each backslash in a regex.
   // The extractor binds the captured digits to answerString.
   val pattern = """.* ([\d]+).*""".r
   val pattern(answerString) = theUltimateAnswer
   val answer = answerString.toInt
   println(answer)

   // Booleans
   val isGreater = 1 > 2
   val isLesser = 1 < 2
   // && and || short-circuit; & and | evaluate both Boolean operands.
   val impossible = isGreater & isLesser
   val anotherWay = isGreater || isLesser

   val picard: String = "Picard"
   val bestCaptain: String = "Picard"
   val isBest: Boolean = picard == bestCaptain

   // EXERCISE
   // Write some code that takes the value of pi, doubles it, and then prints it within a string with
   // three decimal places of precision to the right.
