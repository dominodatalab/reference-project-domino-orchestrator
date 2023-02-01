# This is a sample R/Shiny script to show Domino's App publishing functionality.
# You can publish an app by clicking on "Publish" and selecting "App" in your
# quick-start project.
 
server <- function(input, output) {
  output$distPlot <- renderPlot({
    hist(rnorm(input$obs), col = 'darkgray', border = 'white')
  })
}
 
ui <- fluidPage(sidebarLayout(sidebarPanel(
  sliderInput(
    "obs", "Number of observations:", min = 10, max = 500, value = 100
  )
),
 
mainPanel(plotOutput("distPlot"))))
 
shinyApp(ui = ui, server = server)