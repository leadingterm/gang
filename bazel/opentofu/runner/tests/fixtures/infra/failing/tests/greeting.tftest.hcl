variables {
  name = "world"
}

run "greeting" {
  assert {
    condition     = output.greeting == "bye"
    error_message = "unexpected greeting"
  }
}
