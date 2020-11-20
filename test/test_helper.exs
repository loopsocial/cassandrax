Application.load(:cassandrax)

for app <- Application.spec(:cassandrax, :applications) do
  Application.ensure_all_started(app)
end

ExUnit.configure(exclude: [pending: true])
ExUnit.start()
