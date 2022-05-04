# Importing libraries
import pandas as pd
import numpy as np
import plotly.graph_objects as gp

# Creating instance of the figure
fig = gp.Figure()
  
x_F = np.array([-600, -623, -653, -650, -670, -578, -541, -411, -322, -230])
x_M = np.array([600, 623, 653, 650, 670, 578, 541, 360, 312, 170])
y_age = np.arange(0,10,1)
age_df = pd.DataFrame({'age': y_age, 'x_F': x_F, 'x_M': x_M})

# Adding Male data to the figure
fig.add_trace(gp.Bar(y= y_age, x = x_M, 
                     name = 'Male', 
                     orientation = 'h'))
  
# Adding Female data to the figure
fig.add_trace(gp.Bar(y = y_age, x = x_F,
                     name = 'Female', orientation = 'h'))
  
# Updating the layout for our graph
fig.update_layout(title = 'Population Pyramid of India-2019',
                 title_font_size = 22, barmode = 'relative',
                 bargap = 0.0, bargroupgap = 0,
                 xaxis = dict(tickvals = [-60000000, -40000000, -20000000,
                                          0, 20000000, 40000000, 60000000],
                                
                              ticktext = ['6M', '4M', '2M', '0', 
                                          '2M', '4M', '6M'],
                                
                              title = 'Population in Millions',
                              title_font_size = 14)
                 )
  
fig.show()