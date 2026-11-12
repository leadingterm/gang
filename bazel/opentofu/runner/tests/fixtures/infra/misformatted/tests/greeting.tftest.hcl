variables {
  name = "world"
}

run "greeting" {
  assert {
    condition     = output.greeting == "hello world"
    error_message = "unexpected greeting"
  }
}
