import { BrowserRouter, Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";

import Home from "./pages/Home";
import Resorts from "./pages/Resorts";
import ResortDetail from "./pages/ResortDetail";
import Map from "./pages/Map";
import Api from "./pages/Api";
import ApiDemo from "./pages/ApiDemo";
import UserDashboard from "./pages/UserDashboard";
import Login from "./pages/Login";
import Signup from "./pages/Signup";
import Contact from "./pages/Contact";
import ResortPage from "./pages/ResortPage";

import { AuthProvider } from "./context/AuthContext";
import ProtectedRoute from "./components/ProtectedRoute";

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Navbar />

        <Routes>
          <Route path="/" element={<Home />} />

          <Route path="/resorts" element={<Resorts />} />
          <Route path="/resorts/:id" element={<ResortDetail />} />

          <Route path="/map" element={<Map />} />

          <Route path="/api" element={<Api />} />
          <Route path="/api/demo" element={<ApiDemo />} />

          <Route path="/login" element={<Login />} />
          <Route path="/signup" element={<Signup />} />

          <Route
            path="/user"
            element={
              <ProtectedRoute>
                <UserDashboard />
              </ProtectedRoute>
            }
          />

          <Route path="/contact" element={<Contact />} />
          <Route path="/resort/:name" element={<ResortPage />} />
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}
