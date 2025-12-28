# Unified Dashboard System

A comprehensive Flask application that combines three separate dashboard systems into a single, unified platform for monitoring and managing operations.

## 🚀 Features

### **Main Navigation Dashboard**
- Modern, responsive interface with card-based navigation
- Beautiful gradient design with hover effects and animations
- Quick access to all three dashboard systems

### **IT Dashboard** (`/it-dashboard`)
- Monitor channel status and completion times
- Track logger end times and QC progress
- Identify channels eligible for pulling
- Real-time status indicators with color coding
- Advanced search and filtering capabilities

### **Logger Dashboard** (`/logger-dashboard`)
- Dual source monitoring for Xen and EQ loggers
- Track tagging progress by cluster
- Identify low duration channels
- Auto-refresh functionality (6-minute intervals)
- Comprehensive statistics and progress charts

### **QC Dashboard** (`/qc-dashboard`)
- Quality control monitoring with cluster progress tracking
- View detailed channel information and story data
- Interactive expandable rows for detailed clip information
- Advanced filtering by cluster and channel name
- Real-time progress indicators

## 📁 Project Structure

```
wef/
├── app.py                          # Main unified Flask application
├── requirements.txt                # Python dependencies
├── static/                         # Unified static files
│   ├── favicon.svg
│   ├── favicon.png
│   └── favicon-16x16.svg
├── templates/                      # Unified template files
│   ├── index.html                  # Main navigation dashboard
│   ├── it_dashboard.html           # IT Dashboard interface
│   ├── logger_dashboard.html       # Logger Dashboard interface
│   ├── qc_dashboard.html           # QC Dashboard interface
│   └── stories.html                # QC Stories detail view
└── README.md                       # This documentation
```


## 🛠️ Installation & Setup

### Prerequisites
- Python 3.7+
- pip package manager

### Install Dependencies

**Option 1: Install from requirements.txt (Recommended)**
```bash
pip install -r requirements.txt
```

**Option 2: Install manually**
```bash
pip install flask waitress requests beautifulsoup4 cachetools
```

### Run the Application
```bash
python app.py
```

The application will start on `http://localhost:5000`

## 🌐 Available Routes

### Main Navigation
- `/` - Main dashboard selection page

### Dashboard Routes
- `/it-dashboard` - IT Dashboard for channel status monitoring
- `/logger-dashboard` - Logger Dashboard for dual source monitoring
- `/qc-dashboard` - QC Dashboard for quality control tracking

### API Endpoints
- `/dashboard_data_api` - IT Dashboard data API (POST)
- `/api/dashboard_data` - Logger Dashboard data API (GET)
- `/api/all_clusters_progress` - Logger Dashboard progress API (GET)
- `/channel_data_api` - QC Dashboard data API (POST)
- `/last_clip_times` - QC Dashboard clip times API (GET)
- `/stories` - QC Dashboard stories detail view (GET)

### Static Files
- `/static/<filename>` - Static file serving
- `/favicon.ico` - Favicon redirect

## 🎨 Design Features

- **Responsive Design**: Works seamlessly on desktop and mobile devices
- **Modern UI**: Clean, professional interface with smooth animations
- **Color-Coded Status**: Visual indicators for different states and priorities
- **Interactive Elements**: Hover effects, transitions, and micro-interactions
- **Loading States**: Professional preloaders with progress indicators
- **Navigation**: Easy switching between dashboards with breadcrumb navigation

## 🔧 Technical Implementation

### **Unified Architecture**
- Single Flask application combining all three dashboards
- Shared configuration and utility functions
- Consolidated static file management
- Unified template structure with consistent navigation

### **Key Features**
- **Caching**: TTL cache for Logger Dashboard performance optimization
- **Concurrent Processing**: ThreadPoolExecutor for parallel API calls
- **Error Handling**: Comprehensive error handling and user feedback
- **Session Management**: Persistent sessions for QC Dashboard authentication
- **Data Processing**: Advanced data matching and normalization algorithms

### **External Integrations**
- **Xen API**: Logger data fetching from `http://10.18.80.14:2996`
- **EQ API**: Secondary logger data from `http://10.18.50.26:5000`
- **QC System**: Quality control data from `http://10.18.81.23/tabsons`

## 🧪 Testing

Run the route testing script to verify all endpoints:
```bash
python test_routes.py
```

This will test all main routes and API endpoints, providing status reports for each.

## 📊 Dashboard-Specific Features

### **IT Dashboard**
- Channel status monitoring with real-time updates
- Logger completion time tracking
- QC progress integration
- Smart status classification (Completed, In Progress, Eligible)
- Advanced search and filtering

### **Logger Dashboard**
- Multi-cluster support (National, South, East, West, Hindi Regional)
- Dual logger source monitoring (Xen + EQ)
- Progress tracking with visual indicators
- Auto-refresh with countdown timer
- Low duration channel identification

### **QC Dashboard**
- Cluster-based progress tracking
- Interactive channel details with expandable rows
- Story-level data access
- Advanced filtering capabilities
- Real-time status updates

## 🔒 Configuration

The application uses several configuration constants that can be modified:

- **API Endpoints**: External service URLs and credentials
- **Thresholds**: Completion time thresholds for status determination
- **Channel Mappings**: Channel name aliases and cluster definitions
- **Cache Settings**: TTL cache duration and size limits

## 🚨 Error Handling

The application includes comprehensive error handling:
- Network timeout management
- API failure recovery
- User-friendly error messages
- Graceful degradation when external services are unavailable

## 📈 Performance Optimizations

- **Caching**: TTL cache for frequently accessed data
- **Concurrent Processing**: Parallel API calls for faster data loading
- **Lazy Loading**: On-demand data fetching for better user experience
- **Resource Optimization**: Efficient static file serving

## 🔄 Auto-Refresh Features

- **Logger Dashboard**: 6-minute auto-refresh with visual countdown
- **Real-time Updates**: Dynamic content updates without page reload
- **Smart Refresh**: Preserves user state during updates

## 📱 Mobile Compatibility

All dashboards are fully responsive and work seamlessly on:
- Desktop computers
- Tablets
- Mobile phones
- Various screen sizes and orientations

---

## 🎯 Summary

The Unified Dashboard System successfully consolidates three separate dashboard applications into a single, cohesive platform. It maintains all original functionality while providing:

1. **Simplified Architecture**: Single application instead of three separate ones
2. **Consistent Navigation**: Unified interface with easy switching between dashboards
3. **Shared Resources**: Consolidated static files and templates
4. **Improved User Experience**: Modern design with smooth transitions and interactions
5. **Enhanced Maintainability**: Centralized codebase for easier updates and maintenance

The application is now running successfully on `http://localhost:5000` and all routes have been tested and verified to be working correctly.