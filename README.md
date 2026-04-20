
# Uncle Joe's Coffee Shop - Backend API

This is the backend API for the Uncle Joe's Coffee Shop project. It's built with Python and FastAPI to handle all the database operations for the shop's ordering system.

## Database Structure

The backend interacts with a relational database. The main tables we are using for this project are:

  * **member**: Stores customer accounts, loyalty points, and basic contact info.
  * **location**: The different Uncle Joe's physical store locations.
  * **order**: The main order record (links a member ID to a location ID).
  * **order\_item**: The specific drinks/pastries tied to a specific order.
  * **order\_history**: Tracks the status changes of an order (e.g., placed, making, ready, picked up) so we can show a timeline to the user.

## Endpoints Overview

You can check the Swagger docs for the full request/response schemas, but the routes are basically broken down like this:

  * `/members/` - Create accounts, lookup users, update reward points.
  * `/locations/` - Get a list of stores and their operating hours.
  * `/orders/` - Create a new order, calculate totals, and fetch an order's current status.
  * `/order-items/` - Add or remove specific items from an order.
  * `/history/` - Append new statuses to the order\_history table when the baristas update the screen in the shop.


# notes
Main Old has orginal code created, the Main file was copied from Prof's repo to use as starter code. The toml file was also copied from the Prof's repo. 

# timeline
4/18 - Started work on creating GET, POST, and DELETE endpoints for all tables.
